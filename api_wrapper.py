import subprocess
import threading
import os
import logging
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse, JSONResponse
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="5min Bot API")
bot_process = None
log_lines = []

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
    if bot_process and bot_process.poll() is None:
        return {"status": "healthy", "pid": bot_process.pid}
    return JSONResponse(status_code=503, content={"status": "stopped"})

@app.get("/logs")
def get_logs(lines: int = 50):
    """Return recent logs"""
    recent = log_lines[-lines:] if log_lines else []
    return PlainTextResponse("\n".join(recent))

@app.get("/status")
def status():
    """Get bot status (placeholder)"""
    # Could parse log for latest signals
    return {"mode": "paper", "uptime": "unknown"}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)