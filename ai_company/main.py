"""
AI Universe Swarm Engine — FastAPI Entrypoint
==============================================
Connects the CrewAI multi-agent corporate workforce to the god-mode frontend
via an OpenAI-compatible Server-Sent Events streaming orchestration endpoint.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Iterator, List, Optional

from crewai import Crew
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from ai_company.agents import WORKFORCE_AGENTS
from ai_company.tasks import CORE_TASKS, build_default_crew_inputs

logger = logging.getLogger(__name__)

MASTER_ADMIN_KEY: str = (os.getenv("MASTER_ADMIN_KEY") or "").strip()
SWARM_HOST: str = os.getenv("SWARM_HOST", "0.0.0.0")
SWARM_PORT: int = int(os.getenv("PORT", os.getenv("SWARM_PORT", "8081")))
STREAM_CHUNK_SIZE: int = max(16, int(os.getenv("SWARM_STREAM_CHUNK_SIZE", "48")))

_shutdown_event: asyncio.Event = asyncio.Event()


# ---------------------------------------------------------------------------
# Application lifespan — startup diagnostics and clean shutdown hooks
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(application: FastAPI):
    logger.info("AI Universe Swarm Engine starting on %s:%s", SWARM_HOST, SWARM_PORT)
    logger.info("Workforce agents loaded: %d", len(WORKFORCE_AGENTS))
    logger.info("Core tasks loaded: %d", len(CORE_TASKS))
    if not MASTER_ADMIN_KEY:
        logger.warning(
            "MASTER_ADMIN_KEY is not configured. All orchestration requests will be refused."
        )
    yield
    logger.info("AI Universe Swarm Engine shutting down — draining active streams")
    _shutdown_event.set()
    await asyncio.sleep(0.05)
    logger.info("AI Universe Swarm Engine shutdown complete")


app = FastAPI(
    title="AI Universe Swarm Engine",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("SWARM_CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request schema
# ---------------------------------------------------------------------------


class ChairmanDirectiveSchema(BaseModel):
    directive: str = Field(..., min_length=1, description="Chairman supreme directive text")
    master_key_session: str = Field(..., min_length=1, description="God-mode session master key")


# ---------------------------------------------------------------------------
# Security and SSE utilities
# ---------------------------------------------------------------------------


def validate_master_key_session(master_key_session: str) -> None:
    """Refuse orchestration when the session key does not match server policy."""
    if not MASTER_ADMIN_KEY:
        raise HTTPException(
            status_code=503,
            detail="Swarm security policy unavailable: MASTER_ADMIN_KEY is not configured.",
        )
    provided = (master_key_session or "").strip()
    if not provided or provided != MASTER_ADMIN_KEY:
        raise HTTPException(
            status_code=401,
            detail="Access Refused By Swarm Security Rules",
        )


def format_sse_delta(content: str) -> str:
    """Emit an OpenAI-compatible SSE chunk for god-mode.html stream parsing."""
    payload: Dict[str, Any] = {
        "choices": [
            {
                "delta": {
                    "content": content,
                }
            }
        ]
    }
    return f"data: {json.dumps(payload)}\n\n"


def iter_text_chunks(text: str, chunk_size: int = STREAM_CHUNK_SIZE) -> Iterator[str]:
    """Slice long crew output into incremental stream tokens."""
    if not text:
        return
    buffer = text
    while buffer:
        yield buffer[:chunk_size]
        buffer = buffer[chunk_size:]


def extract_crew_output_text(result: Any) -> str:
    """Normalize CrewAI kickoff return values into plain markdown-safe text."""
    if result is None:
        return ""
    if hasattr(result, "raw") and result.raw:
        return str(result.raw)
    if hasattr(result, "json_dict") and result.json_dict:
        return json.dumps(result.json_dict, indent=2)
    return str(result)


def build_corporate_swarm() -> Crew:
    """Construct the primary eight-agent, four-task corporate workforce crew."""
    return Crew(
        agents=WORKFORCE_AGENTS,
        tasks=CORE_TASKS,
        verbose=True,
    )


def execute_crew_kickoff(directive: str) -> str:
    """Run the blocking CrewAI kickoff cycle in a worker thread."""
    corporate_swarm = build_corporate_swarm()
    kickoff_inputs = build_default_crew_inputs(directive)
    kickoff_inputs["chairman_directive"] = directive
    logger.info("Corporate swarm kickoff initiated for directive length=%d", len(directive))
    result = corporate_swarm.kickoff(inputs=kickoff_inputs)
    output_text = extract_crew_output_text(result)
    logger.info("Corporate swarm kickoff completed. output_length=%d", len(output_text))
    return output_text


async def swarm_orchestration_stream(directive: str) -> AsyncGenerator[str, None]:
    """
    Asynchronous SSE generator that simulates live multi-agent log metrics while
    the Crew executes sequentially, then streams the final PROPOSAL/STRATEGY body.
    """
    pipeline_steps: List[str] = [
        "🔐 **Swarm Security Clearance:** Chairman session validated.\n",
        "⚙️ **Corporate Swarm Initializing:** 8 master-brain agents online.\n",
        "📋 **Task Queue Armed:** CEO orchestration → CMO viral engine → Sales B DM closing → Risk enforcement.\n\n",
        "---\n\n",
        "**[CEO Strategic Management]** Receiving Chairman directive and drafting execution PROPOSAL...\n\n",
    ]

    for step in pipeline_steps:
        if _shutdown_event.is_set():
            yield format_sse_delta("⚠️ Swarm engine shutdown requested. Stream terminated.\n")
            yield "data: [DONE]\n\n"
            return
        yield format_sse_delta(step)
        await asyncio.sleep(0.08)

    task_progress_messages: List[str] = [
        "**[CMO Viral Engine]** Scanning Reddit, Twitter, and TikTok trend vectors...\n\n",
        "**[AI Media Engine]** Queuing FFmpeg and Kokoro-82M TTS render parameters...\n\n",
        "**[Sales B DM Closer]** Routing residential proxy inbox engagement scripts...\n\n",
        "**[Risk Officer]** Injecting 3–7 minute human-like delay windows and Stripe velocity caps...\n\n",
        "---\n\n",
        "**[SWARM OUTPUT]** Final multi-agent synthesis streaming below:\n\n",
    ]

    kickoff_future = asyncio.get_running_loop().run_in_executor(
        None,
        execute_crew_kickoff,
        directive,
    )

    for progress in task_progress_messages:
        if _shutdown_event.is_set():
            yield format_sse_delta("⚠️ Swarm engine shutdown requested. Stream terminated.\n")
            yield "data: [DONE]\n\n"
            return
        yield format_sse_delta(progress)
        await asyncio.sleep(0.12)

    try:
        crew_output = await kickoff_future
    except Exception as exc:
        logger.exception("Corporate swarm kickoff failed")
        error_message = (
            f"**SWARM EXECUTION FAILURE**\n\n"
            f"The corporate workforce encountered a runtime error: {exc}\n"
        )
        yield format_sse_delta(error_message)
        yield "data: [DONE]\n\n"
        return

    if not crew_output.strip():
        crew_output = (
            "## STRATEGY PROPOSAL — CHAIRMAN AUTHORIZATION REQUIRED\n\n"
            "The swarm completed execution but returned an empty synthesis buffer. "
            "Re-issue the directive with tighter revenue, pricing, or funnel constraints.\n\n"
            "> Awaiting **[🟢 APPROVE EXECUTION]** or **[🔴 AMEND STRATEGY]** via god-mode.\n"
        )

    if "PROPOSAL" not in crew_output.upper() and "STRATEGY" not in crew_output.upper():
        crew_output += (
            "\n\n---\n\n"
            "## STRATEGY CHECKPOINT\n\n"
            "This orchestration cycle requires explicit Chairman authorization before "
            "pipeline release. Select **[🟢 APPROVE EXECUTION]** or **[🔴 AMEND STRATEGY]** "
            "in the god-mode control surface.\n"
        )

    for chunk in iter_text_chunks(crew_output):
        if _shutdown_event.is_set():
            yield format_sse_delta("\n\n⚠️ Swarm engine shutdown requested. Stream terminated.\n")
            yield "data: [DONE]\n\n"
            return
        yield format_sse_delta(chunk)
        await asyncio.sleep(0.03)

    yield "data: [DONE]\n\n"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health")
async def health_probe() -> JSONResponse:
    return JSONResponse(
        {
            "service": "AI Universe Swarm Engine",
            "status": "operational" if not _shutdown_event.is_set() else "shutting_down",
            "agents": len(WORKFORCE_AGENTS),
            "tasks": len(CORE_TASKS),
            "security_configured": bool(MASTER_ADMIN_KEY),
        }
    )


@app.post("/api/v1/swarm/orchestrate")
async def orchestrate_swarm(data: ChairmanDirectiveSchema) -> StreamingResponse:
    """
    Chairman god-mode orchestration endpoint.
    Validates master session credentials, executes the corporate Crew, and streams
    OpenAI-compatible SSE deltas for frontend markdown and authorization buttons.
    """
    validate_master_key_session(data.master_key_session)

    directive = data.directive.strip()
    if not directive:
        raise HTTPException(status_code=422, detail="directive must not be empty.")

    if _shutdown_event.is_set():
        raise HTTPException(status_code=503, detail="Swarm engine is shutting down.")

    logger.info("Authorized swarm orchestration request received.")

    return StreamingResponse(
        swarm_orchestration_stream(directive),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# Process signal hooks for clean container and terminal shutdown
# ---------------------------------------------------------------------------


def _handle_termination_signal(signum: int, _frame: Optional[Any]) -> None:
    signal_name = signal.Signals(signum).name
    logger.info("Termination signal received: %s", signal_name)
    _shutdown_event.set()


def register_signal_handlers() -> None:
    signal.signal(signal.SIGINT, _handle_termination_signal)
    signal.signal(signal.SIGTERM, _handle_termination_signal)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _handle_termination_signal)


# ---------------------------------------------------------------------------
# Local execution entrypoint
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("SWARM_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        stream=sys.stdout,
    )
    register_signal_handlers()

    import uvicorn

    uvicorn.run(
        "ai_company.main:app",
        host=SWARM_HOST,
        port=SWARM_PORT,
        reload=False,
        log_level=os.getenv("SWARM_LOG_LEVEL", "info").lower(),
    )
