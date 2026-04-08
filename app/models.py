"""
DataClerk OpenEnv — Pydantic models for actions, observations, and state.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# ─────────────────────────────────────────────
#  Action
# ─────────────────────────────────────────────

class SQLAction(BaseModel):
    """
    The action an agent can take in the DataClerk environment.

    action_type choices
    -------------------
    execute_sql     – Run a SELECT SQL query against the database.
    describe_table  – Get column info for a specific table.
    list_tables     – List all available tables.
    submit_answer   – Submit the final natural-language answer for grading.
    """

    action_type: str = Field(
        description=(
            "One of: 'execute_sql', 'describe_table', 'list_tables', 'submit_answer'"
        )
    )
    sql_query: Optional[str] = Field(
        default=None,
        description="SQL SELECT query to execute (required for execute_sql).",
    )
    table_name: Optional[str] = Field(
        default=None,
        description="Table name to describe (required for describe_table).",
    )
    answer: Optional[str] = Field(
        default=None,
        description="Final answer text (required for submit_answer).",
    )


# ─────────────────────────────────────────────
#  Observation
# ─────────────────────────────────────────────

class QueryResult(BaseModel):
    """Structured result from an executed SQL query."""

    columns: List[str]
    rows: List[List[Any]]
    row_count: int


class SQLObservation(BaseModel):
    """
    Everything the agent sees after each step.

    Fields
    ------
    task_id             – Identifier of the active task.
    task_description    – Full natural-language description of the goal.
    task_hints          – Optional hints for the current task.
    available_tables    – Tables the agent may query.
    schema_summary      – Dict of {table_name: ["col (TYPE)", …]}.
    last_action_type    – The action_type that produced this observation.
    last_query          – The SQL that was last executed (if any).
    last_query_result   – Structured result of the last query (if any).
    last_query_error    – Error message from the last query (if any).
    query_count         – Total successful queries executed this episode.
    step                – Current step number (1-indexed).
    max_steps           – Maximum steps allowed for this task.
    status              – "in_progress" | "submitted" | "timeout".
    """

    task_id: str
    task_description: str
    task_hints: List[str] = []
    available_tables: List[str]
    schema_summary: Dict[str, List[str]]
    last_action_type: Optional[str] = None
    last_query: Optional[str] = None
    last_query_result: Optional[QueryResult] = None
    last_query_error: Optional[str] = None
    query_count: int = 0
    step: int
    max_steps: int
    status: str = "in_progress"


# ─────────────────────────────────────────────
#  State (returned by /state)
# ─────────────────────────────────────────────

class EpisodeState(BaseModel):
    """Full internal state of a running episode (for /state endpoint)."""

    session_id: str
    task_id: str
    step: int
    max_steps: int
    done: bool
    total_reward: float
    submitted_answer: Optional[str] = None
    query_history: List[str] = []
    query_count: int = 0


# ─────────────────────────────────────────────
#  API request / response wrappers
# ─────────────────────────────────────────────

class ResetRequest(BaseModel):
    task_id: Optional[str] = None


class StepRequest(BaseModel):
    session_id: str
    action: SQLAction


class StepResponse(BaseModel):
    observation: SQLObservation
    reward: float
    done: bool
    info: Dict[str, Any]


class ResetResponse(BaseModel):
    session_id: str
    observation: SQLObservation
    done: bool = False
    info: Dict[str, Any] = {}
