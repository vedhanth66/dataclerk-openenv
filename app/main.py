"""
DataClerk OpenEnv — FastAPI server.

Endpoints
---------
GET  /              – metadata
GET  /health        – health check
GET  /tasks         – list all tasks
GET  /tasks/{id}    – single task detail
POST /reset         – start / restart an episode
POST /step          – take one action
GET  /state         – inspect current episode state
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .database import DB_PATH, seed_database
from .environment import DataClerkEnvironment
from .models import (
    EpisodeState,
    ResetRequest,
    ResetResponse,
    SQLAction,
    StepRequest,
    StepResponse,
)
from .tasks import TASKS

# ── Session store (in-memory; one server = one Space instance) ───────────────
_sessions: Dict[str, DataClerkEnvironment] = {}


# ── Lifespan: seed DB on startup ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else ".", exist_ok=True)
    seed_database(DB_PATH)
    # Pre-warm expected-answer cache
    try:
        from .tasks import _expected
        _expected()
    except Exception:
        pass
    yield
    _sessions.clear()


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="DataClerk – Business Data Analysis OpenEnv",
    description=(
        "An OpenEnv-compatible environment where AI agents analyse "
        "a realistic e-commerce SQLite database by writing SQL queries."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_session(session_id: str) -> DataClerkEnvironment:
    env = _sessions.get(session_id)
    if env is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found.")
    return env


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/")
async def root() -> Dict[str, Any]:
    return {
        "name": "DataClerk",
        "version": "1.0.0",
        "description": "Business data analysis environment for SQL-writing AI agents.",
        "tasks": list(TASKS.keys()),
        "endpoints": {
            "reset": "POST /reset",
            "step": "POST /step",
            "state": "GET  /state?session_id=<id>",
            "tasks": "GET  /tasks",
        },
        "openenv_spec": "1.0",
    }


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "healthy"}


@app.get("/tasks")
async def list_tasks() -> Dict[str, Any]:
    return {
        tid: {
            "id": t["id"],
            "name": t["name"],
            "difficulty": t["difficulty"],
            "max_steps": t["max_steps"],
        }
        for tid, t in TASKS.items()
    }


@app.get("/tasks/{task_id}")
async def task_detail(task_id: str) -> Dict[str, Any]:
    t = TASKS.get(task_id)
    if t is None:
        raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found.")
    return {
        "id": t["id"],
        "name": t["name"],
        "difficulty": t["difficulty"],
        "max_steps": t["max_steps"],
        "description": t["description"],
        "hints": t.get("hints", []),
    }


@app.post("/reset")
async def reset(request: Request) -> Dict[str, Any]:
    """
    Start a new episode.

    Body (optional JSON):
        { "task_id": "revenue_analysis" }   ← default if omitted

    Returns ResetResponse with session_id, initial observation, done=false.
    """
    body: Dict[str, Any] = {}
    try:
        body = await request.json()
    except Exception:
        pass

    task_id: str = body.get("task_id") or "revenue_analysis"
    if task_id not in TASKS:
        task_id = "revenue_analysis"

    env = DataClerkEnvironment(task_id)
    _sessions[env.session_id] = env

    obs, info = env.reset()

    return ResetResponse(
        session_id=env.session_id,
        observation=obs,
        done=False,
        info=info,
    ).model_dump()


@app.post("/step")
async def step(request: Request) -> Dict[str, Any]:
    """
    Take one action in an existing episode.

    Body:
        {
            "session_id": "<uuid>",
            "action": {
                "action_type": "execute_sql",
                "sql_query": "SELECT ..."
            }
        }
    """
    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}")

    session_id: str = body.get("session_id", "")
    raw_action: Dict = body.get("action", {})

    if not session_id:
        raise HTTPException(status_code=400, detail="'session_id' is required.")
    if not raw_action:
        raise HTTPException(status_code=400, detail="'action' is required.")

    env = _get_session(session_id)

    try:
        action = SQLAction(**raw_action)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Invalid action: {exc}")

    obs, reward, done, info = env.step_env(action)

    return StepResponse(
        observation=obs,
        reward=reward,
        done=done,
        info=info,
    ).model_dump()


@app.get("/state")
async def state(session_id: str) -> Dict[str, Any]:
    """Return the current internal state of a session."""
    env = _get_session(session_id)
    return env.state().model_dump()


# ── Global exception handler ─────────────────────────────────────────────────

@app.exception_handler(Exception)
async def global_exc_handler(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {type(exc).__name__}: {exc}"},
    )
