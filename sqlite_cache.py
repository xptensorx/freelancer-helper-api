import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Tuple


def _connect_sqlite(path: str, *, autocommit: bool) -> sqlite3.Connection:
    """
    Open a SQLite connection tuned for long-running jobs.
    - timeout/busy_timeout: wait for locks instead of failing immediately
    - WAL: readers won't block writers (and vice versa)
    """
    conn = sqlite3.connect(path, timeout=30, isolation_level=None if autocommit else "")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")  # 30s
    return conn


class SqliteUserCache:
    """
    Persistent user cache backed by SQLite.

    - Fast lookups by user_id
    - Incremental writes (no rewriting huge JSON files)
    - Safe for long-running jobs
    """

    def __init__(self, path: str):
        self.path = path
        folder = os.path.dirname(path) or "."
        os.makedirs(folder, exist_ok=True)
        # Use transactional mode for batch writes (executemany).
        self.conn = _connect_sqlite(path, autocommit=False)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
              user_id INTEGER PRIMARY KEY,
              payload_json TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def get(self, user_id: int) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT payload_json FROM users WHERE user_id = ? LIMIT 1", (int(user_id),)
        )
        row = cur.fetchone()
        if not row:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return None

    def set(self, user_id: int, user_obj: Dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO users (user_id, payload_json) VALUES (?, ?)",
            (int(user_id), json.dumps(user_obj, ensure_ascii=False)),
        )

    def set_many(self, items: Iterable[Tuple[int, Dict[str, Any]]]) -> None:
        rows = [
            (int(uid), json.dumps(obj, ensure_ascii=False)) for uid, obj in items
        ]
        if not rows:
            return
        # Retry a few times if another connection is briefly writing.
        for attempt in range(6):
            try:
                self.conn.executemany(
                    "INSERT OR REPLACE INTO users (user_id, payload_json) VALUES (?, ?)",
                    rows,
                )
                return
            except sqlite3.OperationalError as e:
                if "locked" not in str(e).lower():
                    raise
                time.sleep(min(5.0, 0.2 * (2**attempt)))
        raise sqlite3.OperationalError("database is locked (retries exhausted)")

    def commit(self) -> None:
        self.conn.commit()


class SqliteCompletedFreelancers:
    """
    Track which directory freelancers have been processed.

    Stored in the same SQLite file as `SqliteUserCache` by default (`user_cache.db`),
    but uses a separate table.
    """

    def __init__(self, path: str):
        self.path = path
        folder = os.path.dirname(path) or "."
        os.makedirs(folder, exist_ok=True)
        # Autocommit prevents holding a long write transaction, which can lock the users cache.
        self.conn = _connect_sqlite(path, autocommit=True)

        # Create new schema (and then migrate older schemas if present).
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS completed_freelancers (
              id INTEGER PRIMARY KEY,
              username TEXT,
              display_name TEXT,
              location TEXT,
              completed_at TEXT,
              offset INTEGER,
              index_in_page INTEGER,
              reviewer_count INTEGER,
              status TEXT NOT NULL
            )
            """
        )
        self._migrate_schema()
        # autocommit

    def _migrate_schema(self) -> None:
        """
        Best-effort migration for older DBs.

        - rename freelancer_id -> id
        - rename completed_at_utc -> completed_at
        - add username/display_name/location columns if missing
        """
        try:
            cols = [
                str(r[1])
                for r in self.conn.execute("PRAGMA table_info(completed_freelancers)").fetchall()
            ]
        except Exception:
            return

        # Renames (safe to ignore if already migrated / unsupported)
        if "freelancer_id" in cols and "id" not in cols:
            try:
                self.conn.execute(
                    "ALTER TABLE completed_freelancers RENAME COLUMN freelancer_id TO id"
                )
                cols = [
                    str(r[1])
                    for r in self.conn.execute("PRAGMA table_info(completed_freelancers)").fetchall()
                ]
            except Exception:
                pass

        if "completed_at_utc" in cols and "completed_at" not in cols:
            try:
                self.conn.execute(
                    "ALTER TABLE completed_freelancers RENAME COLUMN completed_at_utc TO completed_at"
                )
                cols = [
                    str(r[1])
                    for r in self.conn.execute("PRAGMA table_info(completed_freelancers)").fetchall()
                ]
            except Exception:
                pass

        # Add missing columns
        for name, ddl in (
            ("username", "ALTER TABLE completed_freelancers ADD COLUMN username TEXT"),
            ("display_name", "ALTER TABLE completed_freelancers ADD COLUMN display_name TEXT"),
            ("location", "ALTER TABLE completed_freelancers ADD COLUMN location TEXT"),
            ("completed_at", "ALTER TABLE completed_freelancers ADD COLUMN completed_at TEXT"),
        ):
            if name not in cols:
                try:
                    self.conn.execute(ddl)
                except Exception:
                    pass

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    @staticmethod
    def _now_utc_iso() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    def mark(
        self,
        *,
        freelancer_id: int,
        username: Optional[str] = None,
        display_name: Optional[str] = None,
        location: Optional[Dict[str, Any]] = None,
        offset: int,
        index_in_page: int,
        reviewer_count: int,
        status: str,
    ) -> None:
        location_json: Optional[str]
        if isinstance(location, dict):
            location_json = json.dumps(location, ensure_ascii=False)
        else:
            location_json = None

        for attempt in range(6):
            try:
                self.conn.execute(
                    """
                    INSERT OR REPLACE INTO completed_freelancers
                      (id, username, display_name, location, completed_at, offset, index_in_page, reviewer_count, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(freelancer_id),
                        username,
                        display_name,
                        location_json,
                        self._now_utc_iso(),
                        int(offset),
                        int(index_in_page),
                        int(reviewer_count),
                        str(status),
                    ),
                )
                return
            except sqlite3.OperationalError as e:
                if "locked" not in str(e).lower():
                    raise
                time.sleep(min(5.0, 0.2 * (2**attempt)))
        raise sqlite3.OperationalError("database is locked (retries exhausted)")

    def commit(self) -> None:
        # autocommit connection; kept for API compatibility
        return


def migrate_json_cache_to_sqlite(json_path: str, sqlite_path: str) -> int:
    """
    One-time helper: migrate existing JsonFileCache { "<id>": {...} } into SQLite.
    Returns number of migrated records.
    """
    if not os.path.exists(json_path):
        return 0

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return 0

    if not isinstance(data, dict):
        return 0

    cache = SqliteUserCache(sqlite_path)
    try:
        migrated = 0
        batch = []
        for k, v in data.items():
            try:
                uid = int(k)
            except Exception:
                continue
            if not isinstance(v, dict):
                continue
            # Skip closed accounts during migration as well
            if bool(v.get("closed")):
                continue
            batch.append((uid, v))
            if len(batch) >= 1000:
                cache.set_many(batch)
                cache.commit()
                migrated += len(batch)
                batch = []
        if batch:
            cache.set_many(batch)
            cache.commit()
            migrated += len(batch)
        return migrated
    finally:
        cache.close()

