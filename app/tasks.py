"""
DataClerk OpenEnv — Task definitions and graders.

Three tasks in order of difficulty:
  1. revenue_analysis       – easy   – top categories by revenue
  2. customer_risk_analysis – medium – churn + lifetime value
  3. business_health_report – hard   – multi-dimensional business analysis
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any, Dict, List, Tuple

from .database import DB_PATH

# ─────────────────────────────────────────────
#  Pre-compute expected answers (once, lazily)
# ─────────────────────────────────────────────

_CACHE: Dict[str, Any] | None = None


def _compute_expected(db_path: str = DB_PATH) -> Dict[str, Any]:
    conn = sqlite3.connect(db_path)
    out: Dict[str, Any] = {}

    # ── Task 1: top 3 categories by revenue (last 180 days, completed) ─────
    rows = conn.execute("""
        SELECT p.category,
               ROUND(SUM(oi.quantity * oi.unit_price), 2) AS revenue
        FROM   orders o
        JOIN   order_items oi ON oi.order_id   = o.id
        JOIN   products    p  ON p.id           = oi.product_id
        WHERE  o.status      = 'completed'
          AND  o.created_at >= date('2025-06-15', '-180 days')
        GROUP  BY p.category
        ORDER  BY revenue DESC
        LIMIT  3
    """).fetchall()
    out["task1"] = {"top_categories": [(r[0], r[1]) for r in rows]}

    # ── Task 2: at-risk customers + avg LTV ─────────────────────────────────
    row = conn.execute("""
        WITH cust_stats AS (
            SELECT customer_id,
                   MAX(created_at)    AS last_order,
                   SUM(total_amount)  AS ltv
            FROM   orders
            WHERE  status = 'completed'
            GROUP  BY customer_id
        )
        SELECT COUNT(*)             AS at_risk_count,
               ROUND(AVG(ltv), 2)  AS avg_ltv
        FROM   cust_stats
        WHERE  last_order < date('2025-06-15', '-90 days')
    """).fetchone()
    out["task2"] = {"at_risk_count": row[0], "avg_ltv": row[1]}

    # ── Task 3a: avg resolution time by priority ────────────────────────────
    rows = conn.execute("""
        SELECT priority,
               ROUND(AVG(julianday(resolved_at) - julianday(created_at)), 2) AS avg_days
        FROM   support_tickets
        WHERE  status IN ('resolved','closed')
          AND  resolved_at IS NOT NULL
        GROUP  BY priority
        ORDER  BY avg_days DESC
    """).fetchall()
    res_times = {r[0]: r[1] for r in rows}
    slowest = max(res_times, key=res_times.get) if res_times else None
    fastest = min(res_times, key=res_times.get) if res_times else None

    # ── Task 3b: category with highest refund rate ───────────────────────────
    row = conn.execute("""
        SELECT p.category,
               ROUND(
                   100.0 * SUM(CASE WHEN o.status = 'refunded' THEN 1 ELSE 0 END)
                   / COUNT(*), 2
               ) AS refund_rate
        FROM   orders      o
        JOIN   order_items oi ON oi.order_id   = o.id
        JOIN   products    p  ON p.id           = oi.product_id
        GROUP  BY p.category
        ORDER  BY refund_rate DESC
        LIMIT  1
    """).fetchone()
    refund_cat = {"category": row[0], "rate": row[1]} if row else None

    # ── Task 3c: customers with 3+ completed orders AND 2+ tickets by tier ──
    rows = conn.execute("""
        SELECT c.tier, COUNT(*) AS cnt
        FROM   customers c
        WHERE  c.id IN (
                   SELECT customer_id FROM orders
                   WHERE  status = 'completed'
                   GROUP  BY customer_id
                   HAVING COUNT(*) >= 3
               )
          AND  c.id IN (
                   SELECT customer_id FROM support_tickets
                   GROUP  BY customer_id
                   HAVING COUNT(*) >= 2
               )
        GROUP  BY c.tier
        ORDER  BY cnt DESC
    """).fetchall()
    tier_breakdown = {r[0]: r[1] for r in rows}
    total_crossover = sum(tier_breakdown.values())

    out["task3"] = {
        "resolution_times": res_times,
        "slowest_priority": slowest,
        "fastest_priority": fastest,
        "highest_refund": refund_cat,
        "tier_breakdown": tier_breakdown,
        "total_crossover": total_crossover,
    }

    conn.close()
    return out


def _expected() -> Dict[str, Any]:
    global _CACHE
    if _CACHE is None:
        _CACHE = _compute_expected()
    return _CACHE


# ─────────────────────────────────────────────
#  Helper: tolerant number extraction
# ─────────────────────────────────────────────

def _numbers_in(text: str) -> List[float]:
    """Extract all numeric values from a string."""
    cleaned = text.replace(",", "")
    return [float(m) for m in re.findall(r"\d+(?:\.\d+)?", cleaned)]


def _approx(val: float, target: float, pct: float = 0.12) -> bool:
    """True if val is within pct% of target."""
    if target == 0:
        return abs(val) < 1e-3
    return abs(val - target) / abs(target) <= pct


# ─────────────────────────────────────────────
#  Grader 1 – Revenue Analysis (Easy)
# ─────────────────────────────────────────────

def _grade_task1(answer: str, query_history: List[str]) -> Tuple[float, Dict]:
    exp = _expected()["task1"]
    top = exp["top_categories"]           # [(cat, revenue), ...]
    al = answer.lower()
    score = 0.0
    bd: Dict[str, Any] = {}

    nums = _numbers_in(answer)

    for rank, (cat, rev) in enumerate(top, 1):
        cat_found = cat.lower() in al
        rev_found = any(_approx(n, rev) for n in nums)

        pts = 0.0
        if cat_found:
            pts += 0.13
        if rev_found:
            pts += 0.08
        score += pts
        bd[f"rank_{rank}"] = {
            "expected": f"{cat}: {rev}",
            "name_found": cat_found,
            "revenue_found": rev_found,
            "points": round(pts, 3),
        }

    # Correct ordering bonus: #1 must appear before #2
    if len(top) >= 2:
        pos1 = al.find(top[0][0].lower())
        pos2 = al.find(top[1][0].lower())
        if pos1 != -1 and pos2 != -1 and pos1 < pos2:
            score += 0.08
            bd["ordering_bonus"] = True

    # SQL quality
    joined = " ".join(query_history).upper()
    if "JOIN" in joined:
        score += 0.05
        bd["used_join"] = True
    if "GROUP BY" in joined:
        score += 0.04
        bd["used_group_by"] = True
    if "ORDER BY" in joined:
        score += 0.03
        bd["used_order_by"] = True

    return round(min(score, 1.0), 4), bd


# ─────────────────────────────────────────────
#  Grader 2 – Customer Risk Analysis (Medium)
# ─────────────────────────────────────────────

def _grade_task2(answer: str, query_history: List[str]) -> Tuple[float, Dict]:
    exp = _expected()["task2"]
    at_risk = exp["at_risk_count"]
    avg_ltv = exp["avg_ltv"]
    al = answer.lower()
    score = 0.0
    bd: Dict[str, Any] = {}

    nums = _numbers_in(answer)

    # At-risk count (30%)
    count_ok = any(_approx(n, float(at_risk), 0.08) for n in nums if n < 500)
    if count_ok:
        score += 0.30
    bd["at_risk_count"] = {"expected": at_risk, "found": count_ok}

    # Average LTV (30%)
    ltv_ok = any(_approx(n, avg_ltv, 0.12) for n in nums if n > 50)
    if ltv_ok:
        score += 0.30
    bd["avg_ltv"] = {"expected": avg_ltv, "found": ltv_ok}

    # Concept accuracy (10%)
    churn_terms = ["90", "inactive", "at-risk", "at risk", "churn", "lapsed"]
    if any(t in al for t in churn_terms):
        score += 0.10
        bd["churn_concept_present"] = True

    # SQL sophistication (30%)
    joined = " ".join(query_history).upper()
    if "JOIN" in joined and "GROUP BY" in joined:
        score += 0.10
        bd["complex_query"] = True
    if "HAVING" in joined:
        score += 0.07
        bd["used_having"] = True
    if "WITH " in joined:
        score += 0.08
        bd["used_cte"] = True
    if "MAX(" in joined and ("90" in joined or "DATE(" in joined):
        score += 0.05
        bd["correct_date_filter"] = True

    return round(min(score, 1.0), 4), bd


# ─────────────────────────────────────────────
#  Grader 3 – Business Health Report (Hard)
# ─────────────────────────────────────────────

def _grade_task3(answer: str, query_history: List[str]) -> Tuple[float, Dict]:
    exp = _expected()["task3"]
    al = answer.lower()
    score = 0.0
    bd: Dict[str, Any] = {}
    nums = _numbers_in(answer)

    # ── Part A: Resolution times (0–25 pts) ─────────────────────────────────
    slowest = exp.get("slowest_priority", "")
    fastest = exp.get("fastest_priority", "")
    res_times = exp.get("resolution_times", {})

    if slowest and slowest.lower() in al:
        score += 0.10
        bd["slowest_priority_found"] = True

    # Check any resolution time value ~correct (within 1.5 days)
    for pri, days in res_times.items():
        if any(abs(n - days) < 1.5 for n in nums):
            score += 0.08
            bd["resolution_time_value_correct"] = {"priority": pri, "days": days}
            break

    if fastest and fastest.lower() in al:
        score += 0.07
        bd["fastest_priority_found"] = True

    # ── Part B: Refund rates (0–25 pts) ──────────────────────────────────────
    hr = exp.get("highest_refund")
    if hr:
        cat_found = hr["category"].lower() in al
        rate_found = any(_approx(n, hr["rate"], 0.20) for n in nums if n < 50)
        if cat_found:
            score += 0.12
            bd["refund_category_found"] = True
        if rate_found:
            score += 0.08
            bd["refund_rate_found"] = {"expected": hr["rate"]}
        if cat_found and rate_found:
            score += 0.05  # bonus for complete part B
            bd["part_B_complete"] = True

    # ── Part C: Tier breakdown (0–25 pts) ────────────────────────────────────
    tier_bd = exp.get("tier_breakdown", {})
    total_cross = exp.get("total_crossover", 0)

    tiers_found = sum(1 for tier in tier_bd if tier.lower() in al)
    if tiers_found >= 2:
        score += 0.10
        bd["tiers_covered"] = tiers_found
    elif tiers_found == 1:
        score += 0.04

    # Total crossover count
    if any(_approx(n, float(total_cross), 0.10) for n in nums if n < 500):
        score += 0.10
        bd["total_crossover_correct"] = {"expected": total_cross}

    # Subquery / CTE usage for complex filter
    joined = " ".join(query_history).upper()
    if "HAVING" in joined and "JOIN" in joined:
        score += 0.05
        bd["complex_filter_used"] = True

    # ── Overall report quality (0–25 pts) ────────────────────────────────────
    n_queries = len(query_history)
    if n_queries >= 3:
        score += 0.06
        bd["ran_multiple_queries"] = n_queries
    if n_queries >= 5:
        score += 0.04

    dimensions = 0
    if any(w in al for w in ["resolution", "ticket", "support", "resolve"]):
        dimensions += 1
    if any(w in al for w in ["refund", "return", "refund rate"]):
        dimensions += 1
    if any(w in al for w in ["tier", "premium", "enterprise", "standard"]):
        dimensions += 1
    if dimensions >= 3:
        score += 0.10
        bd["all_dimensions_covered"] = True
    elif dimensions == 2:
        score += 0.05

    return round(min(score, 1.0), 4), bd


# ─────────────────────────────────────────────
#  Task registry
# ─────────────────────────────────────────────

TASKS: Dict[str, Dict] = {
    "revenue_analysis": {
        "id": "revenue_analysis",
        "name": "Revenue by Category Analysis",
        "difficulty": "easy",
        "max_steps": 8,
        "grader": _grade_task1,
        "description": (
            "You are a business analyst with read-only access to an e-commerce SQLite database.\n\n"
            "TASK: Identify the TOP 3 product categories by total revenue for COMPLETED orders "
            "placed in the last 6 months (180 days from 2025-06-15).\n\n"
            "For each of the top 3 categories provide:\n"
            "  1. Category name\n"
            "  2. Total revenue (rounded to 2 decimal places)\n"
            "List them in DESCENDING order by revenue.\n\n"
            "Database tables: customers, products, orders, order_items, support_tickets\n"
            "SQLite date tip: use date('2025-06-15', '-180 days') for the cutoff.\n\n"
            "Use execute_sql to query, then submit_answer with your findings."
        ),
        "hints": [
            "JOIN orders → order_items → products",
            "Filter WHERE o.status = 'completed' AND o.created_at >= date('2025-06-15','-180 days')",
            "GROUP BY p.category, ORDER BY revenue DESC, LIMIT 3",
        ],
    },
    "customer_risk_analysis": {
        "id": "customer_risk_analysis",
        "name": "At-Risk Customer Identification",
        "difficulty": "medium",
        "max_steps": 12,
        "grader": _grade_task2,
        "description": (
            "You are a customer success analyst investigating churn risk.\n\n"
            "TASK: Find all 'at-risk' customers — customers who have placed at least one "
            "COMPLETED order but whose most recent completed order was MORE THAN 90 DAYS AGO "
            "(relative to 2025-06-15).\n\n"
            "Report:\n"
            "  1. Total COUNT of at-risk customers.\n"
            "  2. Their AVERAGE lifetime value (sum of total_amount for all completed orders "
            "per customer, then average across at-risk customers), rounded to 2 decimal places.\n\n"
            "Submit your answer like: "
            "\"There are X at-risk customers with an average lifetime value of $Y.\"\n\n"
            "Database tables: customers, products, orders, order_items, support_tickets\n"
            "SQLite date tip: date('2025-06-15', '-90 days')"
        ),
        "hints": [
            "Compute MAX(created_at) per customer from completed orders",
            "Filter last_order < date('2025-06-15', '-90 days')",
            "Compute SUM(total_amount) per customer for LTV, then AVG across at-risk set",
            "A CTE (WITH clause) makes this cleaner",
        ],
    },
    "business_health_report": {
        "id": "business_health_report",
        "name": "Comprehensive Business Health Report",
        "difficulty": "hard",
        "max_steps": 20,
        "grader": _grade_task3,
        "description": (
            "You are a senior analyst preparing an executive business health report. "
            "Answer ALL THREE parts below using separate SQL queries.\n\n"

            "── PART A – Support Ticket Resolution ──────────────────────────────────────────\n"
            "For RESOLVED or CLOSED support tickets (where resolved_at is not NULL), "
            "calculate the average resolution time in days per priority level "
            "(use julianday(resolved_at) - julianday(created_at)).\n"
            "Which priority level takes longest to resolve? Which is fastest?\n\n"

            "── PART B – Product Return Rates ───────────────────────────────────────────────\n"
            "Which product CATEGORY has the highest refund rate? "
            "(Refund rate = refunded orders / total orders × 100 for items in that category.)\n"
            "Report the category name and its refund rate (%).\n\n"

            "── PART C – High-Friction Customers ────────────────────────────────────────────\n"
            "Identify customers who have BOTH:\n"
            "  • 3 or more COMPLETED orders, AND\n"
            "  • 2 or more support tickets (any status).\n"
            "Break down the count of such customers by their tier (standard/premium/enterprise) "
            "and report the grand total.\n\n"

            "Submit a structured report covering all three parts.\n\n"
            "Database tables: customers, products, orders, order_items, support_tickets"
        ),
        "hints": [
            "Part A: julianday() for date arithmetic on resolved_at vs created_at",
            "Part B: CASE WHEN o.status='refunded' THEN 1 ELSE 0 END inside SUM/COUNT",
            "Part C: two IN (SELECT … HAVING COUNT(*) >= N) subqueries + GROUP BY tier",
            "Run at least 3 separate focused queries before writing your report",
        ],
    },
}


def get_task(task_id: str) -> Dict:
    """Return task dict, defaulting to the first task if unknown."""
    return TASKS.get(task_id, next(iter(TASKS.values())))
