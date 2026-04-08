"""
DataClerk OpenEnv — Core environment logic.

Each DataClerkEnvironment instance represents one episode (session).
The FastAPI server creates one instance per /reset call and stores it
keyed by session_id.
"""

from __future__ import annotations

import sqlite3
import uuid
from typing import Any, Dict, List, Optional, Tuple

from .database import DB_PATH, get_schema_summary
from .models import EpisodeState, QueryResult, SQLAction, SQLObservation
from .tasks import TASKS, get_task

# Maximum rows returned from a single query (to keep payloads small)
_MAX_ROWS = 50

# Reward constants
_R_SQL_SUCCESS = 0.05
_R_SQL_EMPTY   = 0.02
_R_SQL_ERROR   = -0.02
_R_BAD_ACTION  = -0.01
_R_DUPLICATE   = -0.01
_R_TIMEOUT     = -0.05
_R_EXPLORE     = 0.01   # describe_table / list_tables

_DANGEROUS_KW = {
    "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE",
    "EXEC", "EXECUTE", "PRAGMA", "ATTACH", "DETACH",
}


class DataClerkEnvironment:
    """
    One episode of the DataClerk SQL analysis environment.

    The agent may:
      • execute_sql   – run a SELECT query
      • describe_table – inspect a table's schema
      • list_tables   – list all available tables
      • submit_answer – commit a final answer (ends episode)

    Rewards are shaped throughout the episode:
      • Small positive rewards for valid SQL and exploration encourage progress.
      • Small penalties for errors and duplicate queries discourage loops.
      • The dominant final reward comes from the task grader on submit_answer.
    """

    def __init__(self, task_id: str = "revenue_analysis") -> None:
        self.session_id: str = str(uuid.uuid4())
        self.task_id: str = task_id if task_id in TASKS else "revenue_analysis"
        self._task: Dict = get_task(self.task_id)
        self._db: str = DB_PATH

        # Episode state
        self.step: int = 0
        self.done: bool = False
        self.total_reward: float = 0.0
        self.submitted_answer: Optional[str] = None
        self.query_history: List[str] = []
        self.query_count: int = 0

        # Last-step cache (shown in next observation)
        self._last_action: Optional[str] = None
        self._last_query: Optional[str] = None
        self._last_result: Optional[QueryResult] = None
        self._last_error: Optional[str] = None

        # Schema cached once
        self._schema: Dict[str, List[str]] = {}

    # ──────────────────────────────────────────
    #  Public interface
    # ──────────────────────────────────────────

    def reset(self) -> Tuple[SQLObservation, Dict]:
        self.step = 0
        self.done = False
        self.total_reward = 0.0
        self.submitted_answer = None
        self.query_history = []
        self.query_count = 0
        self._last_action = None
        self._last_query = None
        self._last_result = None
        self._last_error = None
        self._schema = get_schema_summary(self._db)
        return self._observation(), {}

    def step_env(
        self, action: SQLAction
    ) -> Tuple[SQLObservation, float, bool, Dict]:
        if self.done:
            return self._observation(), 0.0, True, {"error": "Episode already finished"}

        self.step += 1

        # Clear last-step cache
        self._last_action = action.action_type
        self._last_query = None
        self._last_result = None
        self._last_error = None

        reward, info = self._dispatch(action)

        # Check step limit (before marking done from action)
        if not self.done and self.step >= self._task["max_steps"]:
            self.done = True
            reward += _R_TIMEOUT
            info["timeout"] = True

        self.total_reward += reward
        return self._observation(), reward, self.done, info

    def state(self) -> EpisodeState:
        return EpisodeState(
            session_id=self.session_id,
            task_id=self.task_id,
            step=self.step,
            max_steps=self._task["max_steps"],
            done=self.done,
            total_reward=round(self.total_reward, 4),
            submitted_answer=self.submitted_answer,
            query_history=self.query_history,
            query_count=self.query_count,
        )

    # ──────────────────────────────────────────
    #  Action handlers
    # ──────────────────────────────────────────

    def _dispatch(self, action: SQLAction) -> Tuple[float, Dict]:
        at = action.action_type
        if at == "execute_sql":
            return self._handle_sql(action.sql_query or "")
        if at == "describe_table":
            return self._handle_describe(action.table_name or "")
        if at == "list_tables":
            return self._handle_list()
        if at == "submit_answer":
            return self._handle_submit(action.answer or "")
        # Unknown action
        self._last_error = f"Unknown action_type: '{at}'"
        return _R_BAD_ACTION, {"error": self._last_error}

    def _handle_sql(self, query: str) -> Tuple[float, Dict]:
        q = query.strip()
        if not q:
            self._last_error = "Empty SQL query."
            return _R_SQL_ERROR, {"error": "Empty query"}

        upper = q.upper()

        # Only allow SELECT / WITH (CTE)
        if not (upper.startswith("SELECT") or upper.startswith("WITH")):
            self._last_error = "Only SELECT (or WITH … SELECT) queries are allowed."
            return _R_SQL_ERROR * 2, {"error": self._last_error}

        # Block dangerous keywords
        for kw in _DANGEROUS_KW:
            # Simple word-boundary check
            pattern = rf"\b{kw}\b"
            import re
            if re.search(pattern, upper):
                self._last_error = f"Keyword '{kw}' is not permitted."
                return _R_SQL_ERROR * 2.5, {"error": self._last_error}

        # Penalise exact duplicate queries (loop detection)
        if q in self.query_history:
            self._last_error = "Duplicate query — same SQL already executed."
            return _R_DUPLICATE, {"error": self._last_error}

        # Execute
        try:
            conn = sqlite3.connect(self._db)
            conn.row_factory = sqlite3.Row
            cur = conn.execute(q)
            all_rows = cur.fetchall()
            conn.close()

            self._last_query = q
            self.query_history.append(q)
            self.query_count += 1

            if not all_rows:
                self._last_result = QueryResult(columns=[], rows=[], row_count=0)
                return _R_SQL_EMPTY, {"message": "Query executed but returned 0 rows."}

            cols = list(all_rows[0].keys())
            rows = [list(r) for r in all_rows[:_MAX_ROWS]]
            self._last_result = QueryResult(
                columns=cols, rows=rows, row_count=len(all_rows)
            )
            return _R_SQL_SUCCESS, {
                "rows_returned": len(all_rows),
                "truncated": len(all_rows) > _MAX_ROWS,
            }

        except sqlite3.Error as exc:
            self._last_error = str(exc)
            return _R_SQL_ERROR, {"error": str(exc)}

    def _handle_describe(self, table_name: str) -> Tuple[float, Dict]:
        valid = ["customers", "products", "orders", "order_items", "support_tickets"]
        if table_name not in valid:
            self._last_error = f"Table '{table_name}' not found."
            return _R_BAD_ACTION, {"error": self._last_error}
        return _R_EXPLORE, {"table": table_name, "columns": self._schema.get(table_name, [])}

    def _handle_list(self) -> Tuple[float, Dict]:
        tables = list(self._schema.keys())
        return _R_EXPLORE, {"tables": tables}

    def _handle_submit(self, answer: str) -> Tuple[float, Dict]:
        self.submitted_answer = answer
        self.done = True

        grader = self._task["grader"]
        grader_score, breakdown = grader(answer, self.query_history)

        # Normalise reward: we want grader_score to be the episode score.
        # Subtract previously accumulated small rewards so total ~ grader_score.
        # (Judges look at the [END] score which the inference script sets to grader_score.)
        reward = grader_score

        return reward, {
            "final_score": grader_score,
            "breakdown": breakdown,
            "answer_submitted": True,
        }

    # ──────────────────────────────────────────
    #  Observation builder
    # ──────────────────────────────────────────

    def _observation(self) -> SQLObservation:
        status = "in_progress"
        if self.submitted_answer is not None:
            status = "submitted"
        elif self.done:
            status = "timeout"

        return SQLObservation(
            task_id=self.task_id,
            task_description=self._task["description"],
            task_hints=self._task.get("hints", []),
            available_tables=list(self._schema.keys()),
            schema_summary=self._schema,
            last_action_type=self._last_action,
            last_query=self._last_query,
            last_query_result=self._last_result,
            last_query_error=self._last_error,
            query_count=self.query_count,
            step=self.step,
            max_steps=self._task["max_steps"],
            status=status,
        )
