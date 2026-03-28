import json
import subprocess
import threading
import os
import logging
import time
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse, JSONResponse
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="5min Bot API")
bot_process = None
log_lines = []
RUNTIME_DIR = Path(os.getenv("RUNTIME_DIR", "/app/data/runtime"))


def runtime_status_payload(runtime_dir: Path = RUNTIME_DIR):
    status_path = Path(runtime_dir) / "status.json"
    if not status_path.exists():
        return {"status": "missing", "runtime_dir": str(runtime_dir)}
    try:
        return json.loads(status_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"status": "corrupt", "runtime_dir": str(runtime_dir)}


def runtime_health_payload(runtime_dir: Path = RUNTIME_DIR, max_heartbeat_age: int = 180):
    payload = runtime_status_payload(runtime_dir)
    heartbeat = payload.get("heartbeat_ts")
    if not heartbeat:
        return {"status": "stopped", "runtime_dir": str(runtime_dir)}
    age = time.time() - float(heartbeat)
    if age <= max_heartbeat_age:
        return {
            "status": "healthy",
            "heartbeat_age_seconds": round(age, 2),
            "run_id": payload.get("run_id"),
            "phase": payload.get("phase"),
        }
    return {
        "status": "stale",
        "heartbeat_age_seconds": round(age, 2),
        "run_id": payload.get("run_id"),
        "phase": payload.get("phase"),
    }

def start_bot():
    """Start the CLI bot as a subprocess"""
    global bot_process, log_lines
    logger.info("Starting bot subprocess...")
    env = os.environ.copy()
    # Ensure we're in the right directory
    env["PYTHONPATH"] = os.getcwd()
    try:
        proc = subprocess.Popen(
            ["python", "cli.py", "run", "--mode", "paper"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            env=env,
            cwd="/app"
        )
        bot_process = proc
        logger.info(f"Bot started with PID {proc.pid}")
        for line in proc.stdout:
            line = line.rstrip()
            log_lines.append(line)
            logger.info(f"Bot: {line}")
            if len(log_lines) > 200:
                log_lines.pop(0)
        exit_code = proc.wait()
        logger.error(f"Bot exited with code {exit_code}")
    except Exception as e:
        logger.exception("Failed to start bot")

@app.on_event("startup")
def startup_event():
    logger.info("API server starting, launching bot thread...")
    thread = threading.Thread(target=start_bot, daemon=True)
    thread.start()

@app.get("/")
def root():
    return {"service": "polymarket-5min-bot", "status": "running"}

@app.get("/health")
def health():
    payload = runtime_health_payload(RUNTIME_DIR)
    status_code = 200 if payload["status"] == "healthy" else 503
    if bot_process and bot_process.poll() is None:
        payload["pid"] = bot_process.pid
    return JSONResponse(status_code=status_code, content=payload)

@app.get("/logs")
def get_logs(lines: int = 50):
    """Return recent logs"""
    recent = log_lines[-lines:] if log_lines else []
    return PlainTextResponse("\n".join(recent))

@app.get("/status")
def status():
    """Get durable runtime status."""
    payload = runtime_status_payload(RUNTIME_DIR)
    if bot_process and bot_process.poll() is None:
        payload["pid"] = bot_process.pid
    return payload

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)