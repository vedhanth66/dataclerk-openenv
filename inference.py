"""
DataClerk OpenEnv — Inference Script
=====================================

Runs an LLM agent against all three DataClerk tasks and emits structured
stdout logs in the mandatory [START] / [STEP] / [END] format.

Environment variables
---------------------
API_BASE_URL   LLM endpoint  (default: HuggingFace router)
MODEL_NAME     Model ID      (default: Qwen/Qwen2.5-72B-Instruct)
HF_TOKEN       API key       (required for HF router)
ENV_BASE_URL   DataClerk server URL (default: http://localhost:7860)

Usage
-----
    # Start the environment first:
    #   uvicorn app.main:app --port 7860
    #
    # Then run:
    python inference.py
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import textwrap
import traceback
from typing import Any, Dict, List, Optional, Tuple

import httpx
from openai import OpenAI

# ─────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────

API_BASE_URL: str = os.getenv("API_BASE_URL", "https://api.groq.com/openai/v1")
MODEL_NAME: str   = os.getenv("MODEL_NAME",   "llama-3.1-8b-instant")
HF_TOKEN: str     = os.getenv("HF_TOKEN", "") or os.getenv("OPENAI_API_KEY", "")
ENV_BASE_URL: str = os.getenv("ENV_BASE_URL", "http://localhost:7860")

BENCHMARK = "dataclerk"

# Task configuration — must match server task IDs
TASK_CONFIGS: Dict[str, Dict] = {
    "revenue_analysis": {
        "max_steps": 8,
        "success_threshold": 0.45,
        "difficulty": "easy",
    },
    "customer_risk_analysis": {
        "max_steps": 12,
        "success_threshold": 0.35,
        "difficulty": "medium",
    },
    "business_health_report": {
        "max_steps": 20,
        "success_threshold": 0.25,
        "difficulty": "hard",
    },
}

# ─────────────────────────────────────────────
#  Prompts
# ─────────────────────────────────────────────

SYSTEM_PROMPT = textwrap.dedent("""
You are an expert SQL data analyst. You interact with a SQLite business database.

Each turn you MUST respond with exactly ONE JSON object — no markdown, no explanation:

  {"action_type": "execute_sql",    "sql_query":  "SELECT ..."}
  {"action_type": "describe_table", "table_name": "<name>"}
  {"action_type": "list_tables"}
  {"action_type": "submit_answer",  "answer": "Your complete findings here"}

Rules:
- Only SELECT queries are allowed (no INSERT/UPDATE/DROP/etc.)
- Use SQLite syntax: date('2025-06-15', '-N days'), julianday(), ROUND(), etc.
- Explore the schema if needed, then write targeted queries
- submit_answer ends the episode — include ALL findings
- Output ONLY the JSON object, nothing else
""").strip()


# ─────────────────────────────────────────────
#  Mandatory log helpers
# ─────────────────────────────────────────────

def log_start(task: str, env: str, model: str) -> None:
    print(f"[START] task={task} env={env} model={model}", flush=True)


def log_step(
    step: int,
    action: str,
    reward: float,
    done: bool,
    error: Optional[str],
) -> None:
    err_val = error.replace("\n", " ")[:120] if error else "null"
    done_val = str(done).lower()
    # Flatten action to single line
    act_clean = action.replace("\n", " ").replace("\r", "")[:250]
    print(
        f"[STEP] step={step} action={act_clean} reward={reward:.2f}"
        f" done={done_val} error={err_val}",
        flush=True,
    )


def log_end(
    success: bool,
    steps: int,
    score: float,
    rewards: List[float],
) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(
        f"[END] success={str(success).lower()} steps={steps}"
        f" score={score:.3f} rewards={rewards_str}",
        flush=True,
    )


# ─────────────────────────────────────────────
#  Action parsing
# ─────────────────────────────────────────────

def _parse_action(raw: str) -> Optional[Dict]:
    """Extract a JSON action dict from model output."""
    raw = raw.strip()

    # Direct parse
    try:
        return json.loads(raw)
    except Exception:
        pass

    # Find first JSON object
    m = re.search(r"\{[\s\S]*?\}", raw)
    if m:
        try:
            return json.loads(m.group())
        except Exception:
            pass

    # Extract SQL if model forgot JSON wrapper
    m = re.search(r"(SELECT\s[\s\S]+?)(?:;|$)", raw, re.IGNORECASE)
    if m:
        return {"action_type": "execute_sql", "sql_query": m.group(1).strip()}

    return None


# ─────────────────────────────────────────────
#  Model interaction
# ─────────────────────────────────────────────

def _format_result(result: Optional[Dict]) -> str:
    if not result:
        return "No result."
    cols = result.get("columns", [])
    rows = result.get("rows", [])
    row_count = result.get("row_count", 0)
    if not cols:
        return "Query returned 0 rows."
    header = " | ".join(str(c) for c in cols)
    sep = "-" * len(header)
    body = "\n".join(" | ".join(str(v) for v in row) for row in rows[:15])
    tail = f"\n... ({row_count} total rows)" if row_count > 15 else ""
    return f"{header}\n{sep}\n{body}{tail}"


def _build_user_message(
    step: int,
    obs: Dict,
    history: List[Tuple[str, str]],
) -> str:
    task_desc = obs.get("task_description", "")
    schema = obs.get("schema_summary", {})
    last_error = obs.get("last_query_error")
    last_result = obs.get("last_query_result")
    last_query = obs.get("last_query")
    max_steps = obs.get("max_steps", 10)

    parts: List[str] = []

    if step == 1:
        parts.append(f"TASK:\n{task_desc}\n")
        if schema:
            schema_lines = []
            for tbl, cols in schema.items():
                schema_lines.append(f"  {tbl}: {', '.join(cols)}")
            parts.append("DATABASE SCHEMA:\n" + "\n".join(schema_lines))
    else:
        # Compact task reminder
        parts.append(f"Task (step {step}/{max_steps}):\n{task_desc[:300]}...")

    if last_query:
        parts.append(f"\nLast SQL:\n{last_query}")

    if last_error:
        parts.append(f"\nERROR: {last_error}")
    elif last_result:
        parts.append(f"\nResult:\n{_format_result(last_result)}")

    parts.append(f"\nStep {step}/{max_steps} — what is your next action?")
    return "\n".join(parts)


def _call_model(
    client: OpenAI,
    step: int,
    obs: Dict,
    history: List[Tuple[str, str]],
) -> Tuple[Dict, str]:
    """Call the LLM and return (parsed_action, raw_text)."""
    user_msg = _build_user_message(step, obs, history)

    messages: List[Dict] = [{"role": "system", "content": SYSTEM_PROMPT}]
    # Inject up to 6 prior turns
    for u, a in history[-6:]:
        messages.append({"role": "user", "content": u})
        messages.append({"role": "assistant", "content": a})
    messages.append({"role": "user", "content": user_msg})

    try:
        resp = client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,
            temperature=0.05,
            max_tokens=512,
            stream=False,
        )
        raw = (resp.choices[0].message.content or "").strip()
    except Exception as exc:
        print(f"[DEBUG] LLM call failed: {exc}", flush=True)
        raw = ""

    action = _parse_action(raw)
    if action is None:
        # Fallback progression
        if step <= 2:
            action = {"action_type": "list_tables"}
        elif step <= 4:
            action = {"action_type": "describe_table", "table_name": "orders"}
        else:
            action = {
                "action_type": "submit_answer",
                "answer": "Analysis incomplete due to model output parsing failure.",
            }

    return action, raw


# ─────────────────────────────────────────────
#  Single-task runner
# ─────────────────────────────────────────────

async def run_task(
    task_id: str,
    client: OpenAI,
    env_url: str,
) -> Tuple[float, bool, int, List[float]]:
    """
    Run one episode of task_id.
    Returns (score, success, steps_taken, rewards_list).
    """
    cfg = TASK_CONFIGS[task_id]
    rewards: List[float] = []
    steps_taken = 0
    score = 0.0
    success = False
    history: List[Tuple[str, str]] = []

    log_start(task=task_id, env=BENCHMARK, model=MODEL_NAME)

    try:
        async with httpx.AsyncClient(timeout=90.0, base_url=env_url) as http:

            # ── Reset ──────────────────────────────────────────────────────
            r = await http.post("/reset", json={"task_id": task_id})
            r.raise_for_status()
            reset_data = r.json()

            session_id: str = reset_data["session_id"]
            obs: Dict = reset_data["observation"]

            # ── Episode loop ───────────────────────────────────────────────
            for step in range(1, cfg["max_steps"] + 1):

                action, raw = _call_model(client, step, obs, history)

                # Execute action
                step_resp = await http.post(
                    "/step",
                    json={"session_id": session_id, "action": action},
                )
                step_resp.raise_for_status()
                step_data = step_resp.json()

                reward: float = step_data.get("reward", 0.0)
                done: bool   = step_data.get("done",   False)
                info: Dict   = step_data.get("info",   {})
                obs          = step_data.get("observation", obs)
                error        = obs.get("last_query_error")

                rewards.append(reward)
                steps_taken = step

                # Track final score when grader fires
                if "final_score" in info:
                    score = float(info["final_score"])

                # Update conversation history
                user_msg = _build_user_message(step, obs, history)
                history.append((user_msg, raw or json.dumps(action)))

                log_step(
                    step=step,
                    action=json.dumps(action),
                    reward=reward,
                    done=done,
                    error=error,
                )

                if done:
                    break

            # If episode timed out without submit, score stays 0
            if score == 0.0 and rewards:
                # Last reward might be the grader score if submit happened
                # (shouldn't reach here normally, but handle edge case)
                score = max(0.0, min(1.0, max(rewards)))

            success = score >= cfg["success_threshold"]

    except Exception as exc:
        print(f"[DEBUG] run_task({task_id}) exception: {exc}", flush=True)
        traceback.print_exc(file=sys.stdout)

    finally:
        log_end(
            success=success,
            steps=steps_taken,
            score=score,
            rewards=rewards,
        )

    return score, success, steps_taken, rewards


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

async def main() -> None:
    client = OpenAI(
        base_url=API_BASE_URL,
        api_key=HF_TOKEN or "dummy-key",
    )

    env_url = ENV_BASE_URL.rstrip("/")
    print(f"[DEBUG] DataClerk inference — model={MODEL_NAME} env={env_url}", flush=True)

    task_ids = list(TASK_CONFIGS.keys())
    summary: List[Dict] = []

    for task_id in task_ids:
        print(f"\n[DEBUG] ── Running task: {task_id} ──", flush=True)
        score, success, steps, _ = await run_task(task_id, client, env_url)
        summary.append(
            {"task": task_id, "score": score, "success": success, "steps": steps}
        )
        print(f"[DEBUG] {task_id}: score={score:.3f} success={success}", flush=True)

    avg = sum(s["score"] for s in summary) / len(summary) if summary else 0.0
    print(f"\n[DEBUG] ── Summary ──", flush=True)
    for s in summary:
        print(f"[DEBUG]   {s['task']:30s}  score={s['score']:.3f}  success={s['success']}", flush=True)
    print(f"[DEBUG]   {'AVERAGE':30s}  score={avg:.3f}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())