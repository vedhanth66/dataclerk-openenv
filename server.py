#!/usr/bin/env python3
"""
DataClerk OpenEnv — Server entry point.

Starts the FastAPI application with Uvicorn.
Can be run directly: python server.py
Or via Docker CMD.
"""

import os
import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        workers=1,
        timeout_keep_alive=30,
        access_log=True,
    )
