"""
Flappy Folder Online Leaderboard & Anti-Cheat Server (FastAPI + SQLite + GitHub Gist Backup)
--------------------------------------------------------------------------------------------
Companion server for `flappy_folder.py` that issues timed run tokens,
verifies run durations and per-point checkpoint timestamps against the game's
exact physics constants, stores verified scores in `leaderboard.db`, and
automatically backs up / restores the leaderboard to a permanent GitHub Gist
so scores are never lost when a free cloud container (e.g., Render) sleeps!

How to Enable Permanent GitHub Gist Storage (on Render or locally):
  1. Go to https://gist.github.com , create a new Secret Gist with a file named
     `leaderboard_backup.json` containing `[]`, and copy the Gist ID from the
     end of its URL (e.g., https://gist.github.com/yourname/<GIST_ID>).
  2. Go to GitHub -> Settings -> Developer settings -> Personal access tokens
     and create a token with the `gist` permission.
  3. On Render (under Environment Variables), set:
       - `GITHUB_TOKEN` = your personal access token
       - `GIST_ID`      = your Gist ID
     (If those variables are not set, the server still works normally using local
     SQLite `leaderboard.db`.)

Local Setup & Run (using Python 3.12 / 3.13):
  py -3.12 -m pip install fastapi uvicorn pydantic
  py -3.12 leaderboard_server.py
"""

import hashlib
import hmac
import json
import os
from pathlib import Path
import random
import secrets
import sqlite3
import threading
import time
from typing import Dict, List, Tuple
import urllib.request

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import uvicorn

# Server & GitHub Gist configuration
DB_PATH = Path(__file__).resolve().parent / "leaderboard.db"
SERVER_SECRET = os.environ.get(
    "FLAPPY_SERVER_SECRET", "flappy_folder_server_hmac_secret_2026"
).encode("utf-8")
CLIENT_SHARED_SALT = "flappy_folder_telemetry_v1"
RUN_EXPIRY_SECONDS = 3600.0  # Expire unfinished run tokens after 1 hour

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
GIST_ID = os.environ.get("GIST_ID", "").strip()
GIST_FILENAME = "leaderboard_backup.json"

# Exact physics constants mirrored from `flappy_folder.py`
BASE_OBSTACLE_SPEED = 220.0
MAX_OBSTACLE_SPEED = 540.0
SPEED_PER_SCORE = 10.5
SPEED_PER_SECOND = 3.2
OBSTACLE_SPACING_PX = 365.0

app = FastAPI(title="Flappy Folder Anti-Cheat Leaderboard API")

# In-memory store of active run sessions: run_id -> dict
ACTIVE_RUNS: Dict[str, dict] = {}


# ---------------------------------------------------------------------------
# Database Setup, Queries & GitHub Gist Cloud Sync
# ---------------------------------------------------------------------------
def init_db() -> None:
    """Create the SQLite leaderboard and used-tokens tables if they do not exist."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leaderboard (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_name TEXT NOT NULL,
                score INTEGER NOT NULL,
                play_time REAL NOT NULL,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_score ON leaderboard (score DESC, play_time ASC)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS used_runs (
                run_id TEXT PRIMARY KEY,
                used_at INTEGER NOT NULL
            )
            """
        )
        conn.commit()


def fetch_top_scores(limit: int = 10) -> List[dict]:
    """Return the top verified scores (highest best score per player)."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT player_name, MAX(score) AS score, MIN(play_time) AS play_time
            FROM leaderboard
            GROUP BY UPPER(player_name)
            ORDER BY score DESC, play_time ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    results: List[dict] = []
    for idx, row in enumerate(rows, start=1):
        results.append(
            {
                "rank": idx,
                "player_name": row["player_name"],
                "score": int(row["score"]),
                "play_time": round(float(row["play_time"]), 2),
            }
        )
    return results


def restore_scores_from_gist() -> None:
    """
    On server startup, if GITHUB_TOKEN and GIST_ID are configured, pull
    `leaderboard_backup.json` from the GitHub Gist and populate SQLite so
    scores persist across free cloud container restarts.
    """
    if not GITHUB_TOKEN or not GIST_ID:
        return

    url = f"https://api.github.com/gists/{GIST_ID}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "FlappyFolder-Leaderboard-Server",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            gist_data = json.loads(resp.read().decode("utf-8"))
        files = gist_data.get("files", {})
        file_obj = files.get(GIST_FILENAME) or next(iter(files.values()), None)
        if not file_obj:
            return
        raw_content = file_obj.get("content", "[]")
        entries = json.loads(raw_content)
        if not isinstance(entries, list):
            return

        with sqlite3.connect(DB_PATH) as conn:
            for item in entries:
                name = str(item.get("player_name", "AGENT"))[:14]
                sc = int(item.get("score", 0))
                tm = float(item.get("play_time", 1.0))
                if sc > 0:
                    conn.execute(
                        """
                        INSERT INTO leaderboard (player_name, score, play_time, created_at)
                        VALUES (?, ?, ?, ?)
                        """,
                        (name, sc, tm, int(time.time())),
                    )
            conn.commit()
        print(f"Restored {len(entries)} leaderboard entries from GitHub Gist {GIST_ID}.")
    except Exception as exc:
        print(f"Note: Could not restore from GitHub Gist ({exc}).")


def sync_scores_to_gist_async() -> None:
    """Push the current Top 50 verified scores to the configured GitHub Gist in the background."""
    if not GITHUB_TOKEN or not GIST_ID:
        return

    top_entries = fetch_top_scores(limit=50)

    def _worker() -> None:
        url = f"https://api.github.com/gists/{GIST_ID}"
        payload = {
            "description": "Flappy Folder Verified Online Leaderboard Backup",
            "files": {
                GIST_FILENAME: {
                    "content": json.dumps(top_entries, indent=2),
                }
            },
        }
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Authorization": f"Bearer {GITHUB_TOKEN}",
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
                "User-Agent": "FlappyFolder-Leaderboard-Server",
            },
            method="PATCH",
        )
        try:
            with urllib.request.urlopen(req, timeout=5.0) as _:
                pass
        except Exception as exc:
            print(f"Note: Could not sync leaderboard to GitHub Gist ({exc}).")

    threading.Thread(target=_worker, daemon=True).start()


def is_run_id_already_used(run_id: str) -> bool:
    """Check if a run_id has already been submitted."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM used_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
    return row is not None


def mark_run_id_used(run_id: str) -> None:
    """Record a run_id as consumed so it can never be replayed."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO used_runs (run_id, used_at) VALUES (?, ?)",
            (run_id, int(time.time())),
        )
        conn.commit()


def record_verified_score(player_name: str, score: int, play_time: float) -> None:
    """Insert a verified run into SQLite and trigger a background GitHub Gist backup."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO leaderboard (player_name, score, play_time, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (player_name, int(score), float(play_time), int(time.time())),
        )
        conn.commit()
    sync_scores_to_gist_async()


# ---------------------------------------------------------------------------
# Anti-Cheat Verification Helpers
# ---------------------------------------------------------------------------
def make_run_token(run_id: str, start_ts: float) -> str:
    """Sign a run_id and start timestamp using the server's private HMAC key."""
    msg = f"{run_id}:{start_ts:.3f}".encode("utf-8")
    return hmac.new(SERVER_SECRET, msg, hashlib.sha256).hexdigest()


def parse_start_ts_from_run_id(run_id: str) -> float:
    """Extract embedded start timestamp from stateless `nonce.timestamp_ms` run_id."""
    if "." in run_id:
        try:
            _, ts_ms_str = run_id.rsplit(".", 1)
            return int(ts_ms_str) / 1000.0
        except Exception:
            pass
    return 0.0


def compute_client_digest(
    run_id: str,
    player_name: str,
    score: int,
    play_time: float,
    flap_count: int,
    point_timestamps: List[float],
) -> str:
    """Recompute the expected client telemetry SHA-256 signature."""
    pts_str = ",".join(f"{t:.3f}" for t in point_timestamps)
    raw = (
        f"{CLIENT_SHARED_SALT}|{run_id}|{player_name}|{score}|"
        f"{play_time:.3f}|{flap_count}|{pts_str}"
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def verify_run_physics(
    score: int,
    client_play_time: float,
    server_elapsed_time: float,
    flap_count: int,
    point_timestamps: List[float],
) -> Tuple[bool, str]:
    """
    Validate that a submitted run obeys wall-clock timing, flap counts,
    and the exact speed & obstacle spacing equations of `flappy_folder.py`.
    """
    if score <= 0:
        return False, "Score must be greater than 0."

    # 1. Wall-clock check: blocks fake instant POST requests and speedhacks
    if abs(server_elapsed_time - client_play_time) > 4.5:
        return (
            False,
            f"Wall-clock mismatch (server={server_elapsed_time:.1f}s vs client={client_play_time:.1f}s).",
        )

    # 2. Checkpoint count must match claimed score
    if len(point_timestamps) != score:
        return (
            False,
            f"Checkpoint count ({len(point_timestamps)}) does not match score ({score}).",
        )

    # 3. Minimum flap count sanity check
    min_required_flaps = max(1, int(client_play_time * 0.45))
    if flap_count < min_required_flaps:
        return False, f"Implausibly low flap count ({flap_count}) for {client_play_time:.1f}s run."

    # 4. Step through every point timestamp and verify physics interval against speed curve
    prev_t = 0.0
    for pt_idx, t in enumerate(point_timestamps):
        if t <= prev_t or t > client_play_time + 0.5:
            return False, f"Invalid timestamp sequence at point {pt_idx + 1}."

        dt = t - prev_t
        max_speed_at_pt = min(
            MAX_OBSTACLE_SPEED,
            BASE_OBSTACLE_SPEED + pt_idx * SPEED_PER_SCORE + t * SPEED_PER_SECOND,
        ) * 1.25

        min_dist = OBSTACLE_SPACING_PX * 0.45 if pt_idx == 0 else OBSTACLE_SPACING_PX * 0.70
        min_dt = min_dist / max_speed_at_pt

        if dt < min_dt:
            return (
                False,
                f"Point {pt_idx + 1} scored too fast ({dt:.2f}s < min {min_dt:.2f}s).",
            )
        prev_t = t

    return True, "Verified"


def cleanup_expired_runs() -> None:
    """Remove stale run tokens older than RUN_EXPIRY_SECONDS."""
    now = time.time()
    expired = [
        rid
        for rid, data in ACTIVE_RUNS.items()
        if now - data["start_time"] > RUN_EXPIRY_SECONDS
    ]
    for rid in expired:
        ACTIVE_RUNS.pop(rid, None)


# ---------------------------------------------------------------------------
# Request / Response Models & API Endpoints
# ---------------------------------------------------------------------------
class SubmitScoreRequest(BaseModel):
    run_id: str
    token: str
    player_name: str = Field(..., min_length=1, max_length=16)
    score: int = Field(..., ge=1, le=10000)
    play_time: float = Field(..., gt=0.5, le=7200.0)
    flap_count: int = Field(..., ge=1, le=100000)
    point_timestamps: List[float]
    client_digest: str


@app.on_event("startup")
def on_startup() -> None:
    init_db()
    restore_scores_from_gist()


@app.post("/api/run/start")
def start_run() -> dict:
    """Issue a one-time server-timed run session token when a game starts."""
    cleanup_expired_runs()
    start_ms = int(time.time() * 1000)
    start_ts = start_ms / 1000.0
    run_id = f"{secrets.token_urlsafe(10)}.{start_ms}"
    token = make_run_token(run_id, start_ts)
    seed = random.randint(100000, 999999999)

    ACTIVE_RUNS[run_id] = {
        "start_time": start_ts,
        "token": token,
        "seed": seed,
    }
    return {
        "run_id": run_id,
        "token": token,
        "seed": seed,
    }


@app.post("/api/run/submit")
def submit_score(payload: SubmitScoreRequest) -> dict:
    """Verify run telemetry and record the score if it passes all anti-cheat checks."""
    if is_run_id_already_used(payload.run_id):
        raise HTTPException(
            status_code=400,
            detail="This run token has already been used.",
        )

    run_record = ACTIVE_RUNS.pop(payload.run_id, None)
    start_ts = (
        run_record["start_time"]
        if run_record is not None
        else parse_start_ts_from_run_id(payload.run_id)
    )
    if start_ts <= 0 or (time.time() - start_ts) > RUN_EXPIRY_SECONDS:
        raise HTTPException(
            status_code=400,
            detail="Invalid or expired run_id.",
        )

    expected_token = make_run_token(payload.run_id, start_ts)
    if not hmac.compare_digest(payload.token, expected_token):
        raise HTTPException(status_code=403, detail="Invalid run session token.")

    clean_name = "".join(
        ch for ch in payload.player_name.strip() if ch.isalnum() or ch in ("_", "-", " ")
    )[:14].strip() or "AGENT"

    expected_digest = compute_client_digest(
        run_id=payload.run_id,
        player_name=clean_name,
        score=payload.score,
        play_time=payload.play_time,
        flap_count=payload.flap_count,
        point_timestamps=payload.point_timestamps,
    )
    if not hmac.compare_digest(payload.client_digest, expected_digest):
        raise HTTPException(status_code=403, detail="Telemetry digest mismatch.")

    server_elapsed = time.time() - start_ts
    ok, reason = verify_run_physics(
        score=payload.score,
        client_play_time=payload.play_time,
        server_elapsed_time=server_elapsed,
        flap_count=payload.flap_count,
        point_timestamps=payload.point_timestamps,
    )
    if not ok:
        raise HTTPException(status_code=422, detail=f"Anti-cheat rejected run: {reason}")

    mark_run_id_used(payload.run_id)
    record_verified_score(clean_name, payload.score, payload.play_time)
    return {
        "status": "verified",
        "message": "Score verified and added to leaderboard!",
        "leaderboard": fetch_top_scores(limit=10),
    }


@app.get("/api/leaderboard")
def get_leaderboard() -> dict:
    """Return the current Top 10 verified online leaderboard."""
    return {
        "leaderboard": fetch_top_scores(limit=10),
    }


if __name__ == "__main__":
    init_db()
    restore_scores_from_gist()
    print("Starting Flappy Folder Anti-Cheat Leaderboard Server on http://127.0.0.1:8000 ...")
    uvicorn.run(app, host="0.0.0.0", port=8000)