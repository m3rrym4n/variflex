from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any


TERMINAL_STATES = {"completed", "failed", "timeout", "quota_exceeded", "cancelled"}


class Database:
    def __init__(self, path: str):
        self.path = path
        self._lock = Lock()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        return connection

    def initialize(self) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY, repo TEXT NOT NULL, issue_number INTEGER NOT NULL,
                    runner TEXT NOT NULL, runner_url TEXT NOT NULL, status TEXT NOT NULL,
                    execution_id TEXT, prompt TEXT, result TEXT, log TEXT NOT NULL DEFAULT '',
                    output_truncated INTEGER NOT NULL DEFAULT 0, error TEXT,
                    resets_at TEXT, session_id TEXT, quota_auto_resume INTEGER NOT NULL DEFAULT 0,
                    branch TEXT, pr_url TEXT,
                    runner_container TEXT,
                    created_at TEXT NOT NULL, started_at TEXT, completed_at TEXT
                )
            """)
            columns = {row[1] for row in db.execute("PRAGMA table_info(tasks)")}
            if "resets_at" not in columns:
                db.execute("ALTER TABLE tasks ADD COLUMN resets_at TEXT")
            if "session_id" not in columns:
                db.execute("ALTER TABLE tasks ADD COLUMN session_id TEXT")
            if "quota_auto_resume" not in columns:
                db.execute("ALTER TABLE tasks ADD COLUMN quota_auto_resume INTEGER NOT NULL DEFAULT 0")
            if "branch" not in columns:
                db.execute("ALTER TABLE tasks ADD COLUMN branch TEXT")
            if "pr_url" not in columns:
                db.execute("ALTER TABLE tasks ADD COLUMN pr_url TEXT")
            if "runner_container" not in columns:
                db.execute("ALTER TABLE tasks ADD COLUMN runner_container TEXT")
            db.execute("""
                CREATE TABLE IF NOT EXISTS runner_queue_state (
                    runner TEXT PRIMARY KEY, active_task_id TEXT,
                    halt_state TEXT, halt_reason TEXT, resumes_at TEXT
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS runner_queue_items (
                    runner TEXT NOT NULL, position INTEGER NOT NULL, task_id TEXT NOT NULL UNIQUE,
                    PRIMARY KEY (runner, position)
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS repository_registry (
                    id INTEGER PRIMARY KEY CHECK (id = 1), config_json TEXT NOT NULL
                )
            """)

    def execute(self, sql: str, parameters: tuple[Any, ...] = ()) -> None:
        with self._lock, self.connect() as db:
            db.execute(sql, parameters)

    def create_task(self, task_id: str, repo: str, issue_number: int, runner: str, runner_url: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.execute(
            "INSERT INTO tasks (id,repo,issue_number,runner,runner_url,status,created_at) VALUES (?,?,?,?,?,'queued',?)",
            (task_id, repo, issue_number, runner, runner_url, now),
        )

    def update(self, task_id: str, **values: Any) -> None:
        if not values:
            return
        columns = ", ".join(f"{key}=?" for key in values)
        self.execute(f"UPDATE tasks SET {columns} WHERE id=?", (*values.values(), task_id))

    def get(self, task_id: str) -> dict[str, Any] | None:
        with self.connect() as db:
            row = db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        return dict(row) if row else None

    def list(self) -> list[dict[str, Any]]:
        with self.connect() as db:
            rows = db.execute("SELECT * FROM tasks ORDER BY created_at DESC").fetchall()
        return [dict(row) for row in rows]

    def save_runner_queue(
        self,
        runner: str,
        pending: list[str],
        active_task_id: str | None,
        halt_state: str | None,
        halt_reason: str | None,
        resumes_at: str | None,
    ) -> None:
        with self._lock, self.connect() as db:
            db.execute(
                """INSERT INTO runner_queue_state
                   (runner,active_task_id,halt_state,halt_reason,resumes_at) VALUES (?,?,?,?,?)
                   ON CONFLICT(runner) DO UPDATE SET active_task_id=excluded.active_task_id,
                   halt_state=excluded.halt_state, halt_reason=excluded.halt_reason,
                   resumes_at=excluded.resumes_at""",
                (runner, active_task_id, halt_state, halt_reason, resumes_at),
            )
            db.execute("DELETE FROM runner_queue_items WHERE runner=?", (runner,))
            if pending:
                placeholders = ",".join("?" for _ in pending)
                db.execute(
                    f"DELETE FROM runner_queue_items WHERE task_id IN ({placeholders})",
                    tuple(pending),
                )
            db.executemany(
                "INSERT INTO runner_queue_items (runner,position,task_id) VALUES (?,?,?)",
                ((runner, position, task_id) for position, task_id in enumerate(pending)),
            )

    def load_runner_queues(self) -> dict[str, dict[str, Any]]:
        with self.connect() as db:
            states = db.execute("SELECT * FROM runner_queue_state").fetchall()
            items = db.execute(
                "SELECT runner,task_id FROM runner_queue_items ORDER BY runner,position"
            ).fetchall()
        pending: dict[str, list[str]] = {}
        for item in items:
            pending.setdefault(item["runner"], []).append(item["task_id"])
        return {
            row["runner"]: {**dict(row), "pending": pending.get(row["runner"], [])}
            for row in states
        }

    def delete_runner_queue(self, runner: str) -> None:
        with self._lock, self.connect() as db:
            db.execute("DELETE FROM runner_queue_items WHERE runner=?", (runner,))
            db.execute("DELETE FROM runner_queue_state WHERE runner=?", (runner,))

    def seed_repo_configs(self, configs: list[dict[str, Any]]) -> None:
        with self._lock, self.connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO repository_registry (id, config_json) VALUES (1, ?)",
                (json.dumps(configs),),
            )

    def load_repo_configs(self) -> list[dict[str, Any]]:
        with self.connect() as db:
            row = db.execute(
                "SELECT config_json FROM repository_registry WHERE id = 1"
            ).fetchone()
        return json.loads(row["config_json"]) if row else []

    def replace_repo_configs(self, configs: list[dict[str, Any]]) -> None:
        with self._lock, self.connect() as db:
            db.execute(
                "INSERT INTO repository_registry (id, config_json) VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET config_json=excluded.config_json",
                (json.dumps(configs),),
            )
