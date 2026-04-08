"""
DataClerk OpenEnv — Test suite (stdlib-only, no pydantic mock needed since pydantic
is installed in the Docker/Space environment; this file works stand-alone too).

Run with: python tests/test_env.py
Or:       python -m pytest tests/ -v
"""

from __future__ import annotations

import os
import sys
import unittest

# ── Path ──────────────────────────────────────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

_TEST_DB = "/tmp/dataclerk_test_suite.db"
os.environ["DB_PATH"] = _TEST_DB


def _ensure_db():
    from app.database import seed_database
    seed_database(_TEST_DB)


_ensure_db()

import app.database as _db_mod
import app.tasks as _tasks_mod

_db_mod.DB_PATH = _TEST_DB
_tasks_mod.DB_PATH = _TEST_DB
_tasks_mod._CACHE = None  # reset so graders read the test DB


# ─────────────────────────────────────────────
#  Database tests
# ─────────────────────────────────────────────

class TestDatabase(unittest.TestCase):

    def test_row_counts(self):
        import sqlite3
        conn = sqlite3.connect(_TEST_DB)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0], 200)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM products").fetchone()[0], 37)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 1800)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM support_tickets").fetchone()[0], 600)
        conn.close()

    def test_deterministic_seeding(self):
        import sqlite3
        path2 = _TEST_DB + ".dup"
        if os.path.exists(path2):
            os.remove(path2)
        from app.database import seed_database
        seed_database(path2)
        c1 = sqlite3.connect(_TEST_DB)
        c2 = sqlite3.connect(path2)
        r1 = c1.execute("SELECT total_amount FROM orders ORDER BY id LIMIT 10").fetchall()
        r2 = c2.execute("SELECT total_amount FROM orders ORDER BY id LIMIT 10").fetchall()
        self.assertEqual(r1, r2)
        c1.close()
        c2.close()
        os.remove(path2)

    def test_schema_summary(self):
        schema = _db_mod.get_schema_summary(_TEST_DB)
        self.assertIn("customers", schema)
        self.assertIn("orders", schema)
        self.assertTrue(any("email" in c for c in schema["customers"]))

    def test_idempotent_seed(self):
        from app.database import seed_database
        seed_database(_TEST_DB)  # second call — must not duplicate
        import sqlite3
        conn = sqlite3.connect(_TEST_DB)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0], 200)
        conn.close()


# ─────────────────────────────────────────────
#  Grader tests
# ─────────────────────────────────────────────

class TestGraders(unittest.TestCase):

    def test_task1_perfect_answer(self):
        answer = (
            "1. Electronics: $330812.35\n"
            "2. Clothing: $84556.61\n"
            "3. Home & Garden: $69583.10"
        )
        queries = [
            "SELECT p.category, SUM(oi.quantity*oi.unit_price) FROM orders o "
            "JOIN order_items oi ON oi.order_id=o.id "
            "JOIN products p ON p.id=oi.product_id "
            "WHERE o.status='completed' GROUP BY p.category ORDER BY 2 DESC LIMIT 3"
        ]
        score, _ = _tasks_mod._grade_task1(answer, queries)
        self.assertGreaterEqual(score, 0.70)

    def test_task1_empty_returns_zero(self):
        score, _ = _tasks_mod._grade_task1("", [])
        self.assertEqual(score, 0.0)

    def test_task1_partial_credit(self):
        answer = "Electronics was top with around 330000 in revenue"
        score, _ = _tasks_mod._grade_task1(answer, [])
        self.assertGreater(score, 0.05)
        self.assertLess(score, 0.60)

    def test_task2_correct_answer(self):
        answer = "26 at-risk customers, average lifetime value $3023.28"
        score, _ = _tasks_mod._grade_task2(answer, ["WITH x AS (SELECT 1 AS n)"])
        self.assertGreaterEqual(score, 0.55)

    def test_task2_empty_returns_zero(self):
        score, _ = _tasks_mod._grade_task2("", [])
        self.assertEqual(score, 0.0)

    def test_task3_complete_report(self):
        answer = (
            "low priority is slowest at 14.25 days. "
            "urgent is fastest at 1.96 days. "
            "Sports has the highest refund rate at 12.21%. "
            "standard: 111, premium: 33, enterprise: 15. Total: 159."
        )
        queries = [
            "SELECT priority, AVG(julianday(resolved_at)-julianday(created_at)) "
            "FROM support_tickets WHERE status IN ('resolved','closed') GROUP BY priority",
            "SELECT p.category, 100.0*SUM(CASE WHEN o.status='refunded' THEN 1 ELSE 0 END)/COUNT(*) "
            "FROM orders o JOIN order_items oi ON oi.order_id=o.id "
            "JOIN products p ON p.id=oi.product_id GROUP BY p.category ORDER BY 2 DESC",
            "SELECT c.tier, COUNT(*) FROM customers c "
            "WHERE c.id IN (SELECT customer_id FROM orders WHERE status='completed' "
            "GROUP BY customer_id HAVING COUNT(*)>=3) "
            "AND c.id IN (SELECT customer_id FROM support_tickets "
            "GROUP BY customer_id HAVING COUNT(*)>=2) GROUP BY c.tier",
            "SELECT * FROM customers LIMIT 3",
            "SELECT * FROM support_tickets LIMIT 3",
        ]
        score, _ = _tasks_mod._grade_task3(answer, queries)
        self.assertGreaterEqual(score, 0.60)

    def test_scores_always_in_bounds(self):
        for ans in ["", "Electronics 999999", "a" * 5000]:
            for fn in [
                _tasks_mod._grade_task1,
                _tasks_mod._grade_task2,
                _tasks_mod._grade_task3,
            ]:
                s, _ = fn(ans, [])
                self.assertGreaterEqual(s, 0.0)
                self.assertLessEqual(s, 1.0)

    def test_three_tasks_registered(self):
        self.assertIn("revenue_analysis", _tasks_mod.TASKS)
        self.assertIn("customer_risk_analysis", _tasks_mod.TASKS)
        self.assertIn("business_health_report", _tasks_mod.TASKS)

    def test_difficulty_progression(self):
        self.assertEqual(_tasks_mod.TASKS["revenue_analysis"]["difficulty"], "easy")
        self.assertEqual(_tasks_mod.TASKS["customer_risk_analysis"]["difficulty"], "medium")
        self.assertEqual(_tasks_mod.TASKS["business_health_report"]["difficulty"], "hard")


# ─────────────────────────────────────────────
#  Environment tests
# ─────────────────────────────────────────────

class TestEnvironment(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        import app.environment as env_mod
        env_mod.DB_PATH = _TEST_DB
        env_mod.get_schema_summary = lambda db=_TEST_DB: _db_mod.get_schema_summary(_TEST_DB)
        cls.env_mod = env_mod
        from app.models import SQLAction
        cls.Action = SQLAction

    def _env(self, task="revenue_analysis"):
        env = self.env_mod.DataClerkEnvironment(task)
        env.reset()
        return env

    def _act(self, **kw):
        return self.Action(**kw)

    # ── reset ──────────────────────────────────────────────────────────────────

    def test_reset_clean_state(self):
        env = self._env()
        self.assertEqual(env.step, 0)
        self.assertEqual(env.query_count, 0)
        self.assertFalse(env.done)
        self.assertEqual(env.total_reward, 0.0)

    def test_reset_returns_observation(self):
        env = self.env_mod.DataClerkEnvironment("revenue_analysis")
        obs, info = env.reset()
        self.assertEqual(obs.task_id, "revenue_analysis")
        self.assertIn("customers", obs.available_tables)
        self.assertIn("orders", obs.schema_summary)

    # ── action types ───────────────────────────────────────────────────────────

    def test_list_tables(self):
        env = self._env()
        _, r, done, info = env.step_env(self._act(action_type="list_tables"))
        self.assertFalse(done)
        self.assertGreater(r, 0)
        self.assertIn("orders", info["tables"])

    def test_describe_table(self):
        env = self._env()
        _, r, done, info = env.step_env(
            self._act(action_type="describe_table", table_name="orders")
        )
        self.assertFalse(done)
        self.assertGreater(r, 0)
        self.assertIn("columns", info)

    def test_describe_unknown_table_penalised(self):
        env = self._env()
        _, r, _, _ = env.step_env(
            self._act(action_type="describe_table", table_name="nonexistent")
        )
        self.assertLess(r, 0)

    def test_execute_sql_count(self):
        env = self._env()
        obs, r, done, _ = env.step_env(
            self._act(action_type="execute_sql", sql_query="SELECT COUNT(*) FROM orders")
        )
        self.assertFalse(done)
        self.assertGreater(r, 0)
        self.assertIsNotNone(obs.last_query_result)
        self.assertEqual(obs.last_query_result.rows[0][0], 1800)

    def test_execute_cte_allowed(self):
        env = self._env()
        cte = ("WITH stats AS (SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id) "
               "SELECT AVG(n) FROM stats")
        obs, r, _, _ = env.step_env(self._act(action_type="execute_sql", sql_query=cte))
        self.assertGreaterEqual(r, 0)
        self.assertIsNotNone(obs.last_query_result)

    def test_sql_error_penalised(self):
        env = self._env()
        _, r, _, _ = env.step_env(
            self._act(action_type="execute_sql", sql_query="SELECT * FROM no_table")
        )
        self.assertLess(r, 0)

    def test_dangerous_sql_blocked(self):
        env = self._env()
        _, r, _, _ = env.step_env(
            self._act(action_type="execute_sql", sql_query="DROP TABLE customers")
        )
        self.assertLess(r, 0)
        import sqlite3
        conn = sqlite3.connect(_TEST_DB)
        count = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        conn.close()
        self.assertEqual(count, 200, "DROP was executed — data was modified!")

    def test_duplicate_query_penalised(self):
        env = self._env()
        sql = "SELECT id FROM customers LIMIT 1"
        _, r1, _, _ = env.step_env(self._act(action_type="execute_sql", sql_query=sql))
        _, r2, _, _ = env.step_env(self._act(action_type="execute_sql", sql_query=sql))
        self.assertGreater(r1, 0)
        self.assertLess(r2, 0)

    def test_submit_ends_episode(self):
        env = self._env()
        _, r, done, info = env.step_env(
            self._act(action_type="submit_answer",
                      answer="Electronics 330812, Clothing 84556, Home & Garden 69583")
        )
        self.assertTrue(done)
        self.assertIn("final_score", info)
        self.assertGreaterEqual(info["final_score"], 0.0)
        self.assertLessEqual(info["final_score"], 1.0)

    def test_submit_good_answer_scores_high(self):
        env = self._env()
        # Run a good query first
        env.step_env(self._act(
            action_type="execute_sql",
            sql_query=(
                "SELECT p.category, ROUND(SUM(oi.quantity*oi.unit_price),2) "
                "FROM orders o JOIN order_items oi ON oi.order_id=o.id "
                "JOIN products p ON p.id=oi.product_id "
                "WHERE o.status='completed' AND o.created_at>=date('2025-06-15','-180 days') "
                "GROUP BY p.category ORDER BY 2 DESC LIMIT 3"
            )
        ))
        _, r, _, info = env.step_env(self._act(
            action_type="submit_answer",
            answer="1. Electronics $330812.35 2. Clothing $84556.61 3. Home & Garden $69583.10"
        ))
        self.assertGreaterEqual(info["final_score"], 0.50)

    # ── episode lifecycle ──────────────────────────────────────────────────────

    def test_timeout_at_max_steps(self):
        env = self.env_mod.DataClerkEnvironment("revenue_analysis")  # max_steps=8
        env.reset()
        done = False
        for i in range(10):
            _, _, done, _ = env.step_env(
                self._act(action_type="execute_sql", sql_query=f"SELECT {i+1}")
            )
            if done:
                self.assertLessEqual(i + 1, 8)
                break
        self.assertTrue(done)

    def test_step_after_done_is_noop(self):
        env = self._env()
        env.step_env(self._act(action_type="submit_answer", answer="done"))
        _, r2, done2, _ = env.step_env(self._act(action_type="list_tables"))
        self.assertTrue(done2)
        self.assertEqual(r2, 0.0)

    def test_state_tracks_progress(self):
        env = self._env()
        env.step_env(self._act(action_type="list_tables"))
        env.step_env(self._act(action_type="execute_sql", sql_query="SELECT * FROM products LIMIT 5"))
        state = env.state()
        self.assertEqual(state.step, 2)
        self.assertEqual(state.query_count, 1)
        self.assertFalse(state.done)

    def test_all_tasks_work(self):
        for task_id in ["revenue_analysis", "customer_risk_analysis", "business_health_report"]:
            env = self.env_mod.DataClerkEnvironment(task_id)
            obs, _ = env.reset()
            self.assertEqual(obs.task_id, task_id)
            self.assertGreater(obs.max_steps, 0)

    def test_unknown_action_penalised(self):
        env = self._env()
        _, r, _, _ = env.step_env(self._act(action_type="fly_to_moon"))
        self.assertLess(r, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
