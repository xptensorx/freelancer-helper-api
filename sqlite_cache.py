import json
import os
import sqlite3
from typing import Any, Dict, Iterable, Optional, Tuple


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
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
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
        self.conn.executemany(
            "INSERT OR REPLACE INTO users (user_id, payload_json) VALUES (?, ?)", rows
        )

    def commit(self) -> None:
        self.conn.commit()


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

