---
title: DataClerk OpenEnv
emoji: 🗄️
colorFrom: blue
colorTo: green
sdk: docker
app_port: 7860
tags:
  - openenv
  - sql
  - data-analysis
  - business-intelligence
  - agent-benchmark
  - rl
pinned: false
---
# DataClerk — Business Data Analysis OpenEnv

[![OpenEnv](https://img.shields.io/badge/OpenEnv-1.0-blue)](https://github.com/openenv)
[![HuggingFace](https://img.shields.io/badge/🤗-HuggingFace_Space-yellow)](https://huggingface.co/spaces)
[![Python 3.11](https://img.shields.io/badge/python-3.11-green)](https://python.org)

> **An OpenEnv-compatible environment where AI agents analyse a realistic
> e-commerce database by writing SQL queries and producing actionable business reports.**

---

## Overview

DataClerk places an agent in the role of a business data analyst with **read-only
access to a SQLite e-commerce database**. The agent must:

1. Explore the database schema
2. Write SQL queries to extract information
3. Synthesise results into a natural-language answer
4. Submit the answer for automated grading

Every task has a programmatic grader that awards **partial credit** for each
correct insight, so reward is dense across the episode — not just a binary
pass/fail at the end.

### Why DataClerk?

| Property | Detail |
|---|---|
| **Real-world utility** | Every company has analysts extracting insights from databases. Training agents on this task has direct commercial value. |
| **Unambiguous grading** | SQL queries either return the correct answer or they don't — graders are fully deterministic. |
| **Partial-credit rewards** | Finding 2 of 3 correct categories earns ~65 % of the score for that part. |
| **Difficulty range** | Easy = single GROUP BY · Medium = CTEs + date maths · Hard = 3-part multi-join report |
| **Novelty** | SQL analysis is absent from existing OpenEnv benchmarks; DataClerk fills this gap. |

---

## Database Schema

All data is seeded deterministically (random seed 42, fixed reference date
`2025-06-15`) so grader answers are **identical on every run**.

```
customers      (200 rows)  id, name, email, city, country, tier, created_at
products        (37 rows)  id, name, category, base_price, stock_quantity
orders        (1800 rows)  id, customer_id, status, total_amount, created_at
order_items   (~4000 rows) id, order_id, product_id, quantity, unit_price
support_tickets (600 rows) id, customer_id, category, priority, status,
                           created_at, resolved_at
```

Product categories: **Electronics · Clothing · Food & Beverage · Sports · Home & Garden**

Customer tiers: **standard (60 %) · premium (30 %) · enterprise (10 %)**

Order statuses: `completed` (80 %) · `refunded` (10 %) · `pending` (10 %)

---

## Action Space

The agent sends one JSON action per step:

| `action_type` | Additional fields | Description |
|---|---|---|
| `execute_sql` | `sql_query` | Run any `SELECT` (or `WITH … SELECT`) query |
| `describe_table` | `table_name` | Get column info for a table |
| `list_tables` | — | List all available tables |
| `submit_answer` | `answer` | Submit final answer (ends episode) |

**Only SELECT queries are permitted.** Dangerous keywords (`DROP`, `INSERT`, `UPDATE`, etc.) are blocked.

### Example action

```json
{
  "action_type": "execute_sql",
  "sql_query": "SELECT p.category, ROUND(SUM(oi.quantity * oi.unit_price), 2) AS revenue FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id WHERE o.status = 'completed' GROUP BY p.category ORDER BY revenue DESC LIMIT 3"
}
```

---

## Observation Space

```json
{
  "task_id":           "revenue_analysis",
  "task_description":  "...",
  "task_hints":        ["...", "..."],
  "available_tables":  ["customers", "products", "orders", "order_items", "support_tickets"],
  "schema_summary":    {"customers": ["id (INTEGER)", "name (TEXT)", "..."], "...": "..."},
  "last_action_type":  "execute_sql",
  "last_query":        "SELECT ...",
  "last_query_result": {"columns": ["category", "revenue"], "rows": [[...]], "row_count": 5},
  "last_query_error":  null,
  "query_count":       2,
  "step":              3,
  "max_steps":         8,
  "status":            "in_progress"
}
```

---

## Tasks

### Task 1 — Revenue by Category Analysis *(easy)*

**Goal:** Identify the top 3 product categories by total revenue for **completed** orders
in the last 6 months (180 days), listed in descending order with revenue figures.

**Max steps:** 8  
**Key SQL concepts:** `JOIN`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`

**Grading breakdown:**
- Category name found in answer: 13 pts × 3
- Revenue figure ≈ correct (±12 %): 8 pts × 3
- Correct descending order: 8 pts
- Used `JOIN` / `GROUP BY` / `ORDER BY`: up to 12 pts bonus

---

### Task 2 — At-Risk Customer Identification *(medium)*

**Goal:** Find customers whose most recent **completed** order was > 90 days ago.
Report the total **count** of such customers and their **average lifetime value** (avg total spend).

**Max steps:** 12  
**Key SQL concepts:** CTE / subquery, `MAX()`, date arithmetic, `HAVING`, `AVG()`

**Grading breakdown:**
- At-risk count ≈ correct (±8 %): 30 pts
- Average LTV ≈ correct (±12 %): 30 pts
- Churn concept present in answer: 10 pts
- Used complex SQL (CTE, HAVING, date filter): up to 30 pts bonus

---

### Task 3 — Comprehensive Business Health Report *(hard)*

**Goal:** Answer all three parts:

- **Part A** — Average support ticket resolution time (days) per priority. Which is slowest / fastest?
- **Part B** — Which product category has the highest refund rate (%)? What is the rate?
- **Part C** — Customers with ≥ 3 completed orders **AND** ≥ 2 support tickets: count by tier + total.

**Max steps:** 20  
**Key SQL concepts:** `julianday()`, `CASE WHEN`, nested subqueries, `HAVING`, multi-table JOINs

**Grading breakdown:**
- Slowest priority identified: 10 pts · resolution time value: 8 pts · fastest priority: 7 pts
- Highest-refund category: 12 pts · refund rate value: 8 pts · complete Part B: 5 pts bonus
- Tiers covered: up to 10 pts · total crossover count: 10 pts
- Query depth / comprehensiveness: up to 20 pts bonus

---

## Reward Function

| Event | Reward |
|---|---|
| Successful SQL query | +0.05 |
| Query returns 0 rows | +0.02 |
| SQL error | −0.02 |
| Dangerous keyword | −0.05 |
| Duplicate (identical) query | −0.01 |
| `describe_table` / `list_tables` | +0.01 |
| Timeout (step limit reached) | −0.05 |
| `submit_answer` (grader score) | **0.0 – 1.0** |

The end-of-episode grader score dominates. Intermediate rewards give the agent
a signal throughout the trajectory and penalise unproductive behaviour (loops,
bad SQL).

---

## Baseline Scores

Measured with `Qwen/Qwen2.5-72B-Instruct` via HuggingFace Inference API:

| Task | Difficulty | Baseline Score |
|---|---|---|
| revenue_analysis | easy | ~0.72 |
| customer_risk_analysis | medium | ~0.51 |
| business_health_report | hard | ~0.31 |
| **Average** | | **~0.51** |

---

## Setup

### Option A — Docker (recommended)

```bash
docker build -t dataclerk .
docker run -p 7860:7860 -e HF_TOKEN=$HF_TOKEN dataclerk
```

### Option B — Local Python

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 7860 --reload
```

### Run the inference script

```bash
export HF_TOKEN=your_hf_token
export API_BASE_URL=https://router.huggingface.co/v1
export MODEL_NAME=Qwen/Qwen2.5-72B-Instruct
export ENV_BASE_URL=http://localhost:7860

python inference.py
```

---

## API Reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Environment metadata |
| `GET` | `/health` | Health check |
| `GET` | `/tasks` | List all tasks |
| `GET` | `/tasks/{id}` | Task detail |
| `POST` | `/reset` | Start episode. Body: `{"task_id": "..."}` |
| `POST` | `/step` | Take action. Body: `{"session_id": "...", "action": {...}}` |
| `GET` | `/state?session_id=...` | Inspect episode state |

### Quick test with curl

```bash
# Start episode
SESSION=$(curl -s -X POST http://localhost:7860/reset \
  -H 'Content-Type: application/json' \
  -d '{"task_id":"revenue_analysis"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")

# Execute a query
curl -s -X POST http://localhost:7860/step \
  -H 'Content-Type: application/json' \
  -d "{\"session_id\":\"$SESSION\",\"action\":{\"action_type\":\"list_tables\"}}"
```

---

## Project Structure

```
dataclerk-openenv/
├── Dockerfile
├── README.md
├── openenv.yaml          ← OpenEnv spec
├── requirements.txt
├── inference.py          ← Baseline inference script
└── app/
    ├── __init__.py
    ├── main.py           ← FastAPI server
    ├── models.py         ← Pydantic schemas
    ├── database.py       ← SQLite setup & deterministic seeding
    ├── environment.py    ← Episode logic (step/reset/state)
    └── tasks.py          ← Task definitions + graders
```

---

## HuggingFace Space Deployment

This repo is configured as a **Docker Space** on HuggingFace Hub. The Space:

- Runs on port **7860** (HF default)
- Starts a single Uvicorn worker (fits within 2 vCPU / 8 GB RAM)
- Seeds the SQLite database on first startup (takes < 2 s)
- Responds to `POST /reset` with HTTP 200 for the OpenEnv validator

Tags: `openenv`, `sql`, `data-analysis`, `business-intelligence`

---

## License

MIT
