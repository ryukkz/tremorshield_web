"""
TREMORSHIELD - Web Data Collection Backend

Browser replacement for mouse_logger.py + task_protocol_gui.py.
Participants open a link, do the task protocol in their browser, and their
mouse events stream to this server over a WebSocket as they go (so almost
nothing is lost if a tab crashes mid-session, unlike a purely local buffer).

Output schema is IDENTICAL to the desktop pipeline, so synthetic_tremor.py
and everything downstream needs zero changes:
timestamp,x,y,dx,dy,velocity,acceleration,event,button,drag,task,session_id,user_id
"""

import csv
import io
import os
import shutil
import smtplib
import time
import uuid
import zipfile
from dataclasses import dataclass, field
from email.message import EmailMessage
from pathlib import Path
from threading import Lock

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "raw"
FRONTEND_DIR = BASE_DIR / "frontend"
DATA_DIR.mkdir(parents=True, exist_ok=True)

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "changeme")
SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL")

HEADER = ["timestamp", "x", "y", "dx", "dy", "velocity", "acceleration",
          "event", "button", "drag", "task", "session_id", "user_id"]

# ---------------------------------------------------------------------------
# Same task table as task_protocol_gui.py (minus Tkinter rendering details;
# the frontend draws targets itself). Single source of truth served to the
# browser via GET /api/tasks so instructions/durations never drift.
# ---------------------------------------------------------------------------
TASK_ORDER = [
    "normal", "fast", "slow", "click", "double_click",
    "drag", "target_selection", "precision", "idle",
]

TASK_INFO = {
    "normal": {"duration": 60, "instructions": "Move the mouse freely and naturally around the screen.\nNo target — just move as you normally would."},
    "fast": {"duration": 60, "instructions": "Move the mouse pointer quickly back and forth across the screen."},
    "slow": {"duration": 60, "instructions": "Move the mouse pointer slowly and deliberately,\nas if being very careful and precise."},
    "click": {"duration": 60, "instructions": "Click the red target circle repeatedly.\nClick at whatever pace feels natural."},
    "double_click": {"duration": 60, "instructions": "Double-click the red target circle repeatedly."},
    "drag": {"duration": 60, "instructions": "Click and drag the blue square into the green box.\nRelease it inside, then drag it back out and repeat."},
    "target_selection": {"duration": 90, "instructions": "Click each orange target as soon as it appears.\nTargets vary in size and distance."},
    "precision": {"duration": 60, "instructions": "Make small, careful movements and clicks\nconfined to the marked box in the center."},
    "idle": {"duration": 30, "instructions": "Rest your hand on the mouse.\nDo not move it intentionally for the full duration."},
}


class StartSessionRequest(BaseModel):
    user_id: str
    session_id: int


@dataclass
class ServerSession:
    session_uuid: str
    user_id: str
    session_id: int
    out_dir: Path
    rows: list = field(default_factory=list)
    lock: Lock = field(default_factory=Lock)
    drag_active: bool = False
    last_t: float = None
    last_x: float = None
    last_y: float = None
    last_v: float = None
    finished: bool = False

    def record(self, t, x, y, event, button="none"):
        with self.lock:
            if event == "press":
                self.drag_active = True
            elif event == "release":
                self.drag_active = False

            if self.last_t is None:
                dx = dy = 0.0
                velocity = 0.0
                acceleration = 0.0
            else:
                dt = max(t - self.last_t, 1e-4)
                dx = x - self.last_x
                dy = y - self.last_y
                velocity = (dx ** 2 + dy ** 2) ** 0.5 / dt
                acceleration = (velocity - (self.last_v or 0.0)) / dt

            self.rows.append([
                t, x, y, dx, dy, velocity, acceleration,
                event, button, self.drag_active, self.current_task,
                self.session_id, self.user_id,
            ])
            self.last_t, self.last_x, self.last_y, self.last_v = t, x, y, velocity

    current_task: str = "idle"

    def pop_rows_for_task(self, task_name):
        with self.lock:
            matching = [r for r in self.rows if r[10] == task_name]
            self.rows = [r for r in self.rows if r[10] != task_name]
        return matching

    def write_csv(self, rows, task_name):
        self.out_dir.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%dT%H%M%S")
        path = self.out_dir / f"{self.user_id}_s{self.session_id:02d}_{task_name}_{stamp}.csv"
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(HEADER)
            writer.writerows(rows)
        return path


SESSIONS: dict[str, ServerSession] = {}
SESSIONS_LOCK = Lock()

app = FastAPI(title="TREMORSHIELD Web Collector")


@app.get("/api/tasks")
def get_tasks():
    return {"task_order": TASK_ORDER, "task_info": TASK_INFO}


@app.post("/api/start_session")
def start_session(req: StartSessionRequest):
    session_uuid = uuid.uuid4().hex
    out_dir = DATA_DIR / f"{req.user_id}_s{req.session_id:02d}_{session_uuid[:8]}"
    sess = ServerSession(
        session_uuid=session_uuid, user_id=req.user_id,
        session_id=req.session_id, out_dir=out_dir,
    )
    with SESSIONS_LOCK:
        SESSIONS[session_uuid] = sess
    return {"session_uuid": session_uuid}


@app.websocket("/ws/{session_uuid}")
async def ws_endpoint(websocket: WebSocket, session_uuid: str):
    await websocket.accept()
    sess = SESSIONS.get(session_uuid)
    if sess is None:
        await websocket.send_json({"type": "error", "message": "unknown session"})
        await websocket.close()
        return

    try:
        while True:
            msg = await websocket.receive_json()
            mtype = msg.get("type")

            if mtype == "events":
                task = msg.get("task", "idle")
                sess.current_task = task
                for ev in msg.get("events", []):
                    sess.record(
                        t=ev["t"], x=ev["x"], y=ev["y"],
                        event=ev["event"], button=ev.get("button", "none"),
                    )

            elif mtype == "end_task":
                task = msg.get("task")
                rows = sess.pop_rows_for_task(task)
                path = sess.write_csv(rows, task)
                await websocket.send_json({"type": "task_saved", "task": task, "rows": len(rows), "path": str(path)})

            elif mtype == "finish":
                # flush anything left (e.g. idle task not explicitly ended)
                remaining_tasks = {r[10] for r in sess.rows}
                for t in remaining_tasks:
                    rows = sess.pop_rows_for_task(t)
                    sess.write_csv(rows, t)
                sess.finished = True
                zip_path = zip_session(sess)
                emailed = try_email_zip(zip_path, sess)
                await websocket.send_json({
                    "type": "finished",
                    "emailed": emailed,
                    "message": "Session data saved on the server." + (" A copy was emailed to the study admin." if emailed else ""),
                })
                break

    except WebSocketDisconnect:
        # Participant closed the tab mid-task. Whatever was already sent in
        # earlier "events" batches is safely on disk/in memory; only the
        # batch currently in flight (at most ~150ms of movement) is lost.
        if not sess.finished:
            remaining_tasks = {r[10] for r in sess.rows}
            for t in remaining_tasks:
                rows = sess.pop_rows_for_task(t)
                sess.write_csv(rows, t)


def zip_session(sess: ServerSession) -> Path:
    zip_path = sess.out_dir.with_suffix(".zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for csv_file in sorted(sess.out_dir.glob("*.csv")):
            zf.write(csv_file, arcname=csv_file.name)
    return zip_path


def try_email_zip(zip_path: Path, sess: ServerSession) -> bool:
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS and ADMIN_EMAIL):
        return False
    try:
        msg = EmailMessage()
        msg["Subject"] = f"TREMORSHIELD data — {sess.user_id} session {sess.session_id}"
        msg["From"] = SMTP_USER
        msg["To"] = ADMIN_EMAIL
        msg.set_content(f"Attached: session data for participant {sess.user_id}, session {sess.session_id}.")
        with open(zip_path, "rb") as f:
            msg.add_attachment(f.read(), maintype="application", subtype="zip", filename=zip_path.name)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        return True
    except Exception as e:
        print(f"[email] failed to send {zip_path}: {e}")
        return False


# ---------------------------------------------------------------------------
# Admin endpoints — list/download session archives without relying on email.
# ---------------------------------------------------------------------------
@app.get("/admin/sessions")
def list_sessions(token: str = Query(...)):
    if token != ADMIN_TOKEN:
        raise HTTPException(403, "bad token")
    zips = sorted(DATA_DIR.glob("*.zip"))
    return {"sessions": [z.name for z in zips]}


@app.get("/admin/download/{name}")
def download_session(name: str, token: str = Query(...)):
    if token != ADMIN_TOKEN:
        raise HTTPException(403, "bad token")
    path = DATA_DIR / name
    if not path.exists() or path.suffix != ".zip":
        raise HTTPException(404, "not found")
    return FileResponse(path, filename=name)


@app.get("/admin/download_all")
def download_all(token: str = Query(...)):
    if token != ADMIN_TOKEN:
        raise HTTPException(403, "bad token")
    buf_path = Path("/tmp/tremorshield_all.zip")
    with zipfile.ZipFile(buf_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for z in DATA_DIR.glob("*.zip"):
            zf.write(z, arcname=z.name)
    return FileResponse(buf_path, filename="tremorshield_all_sessions.zip")


# Serve the frontend last so /api and /admin and /ws take priority.
app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
