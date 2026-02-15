from __future__ import annotations

"""
Backfill `public.clients.reg_at` from `public.clients.joined_at`.

This script is intentionally standalone (not part of `lead_generator.py`).

Prereqs:
- `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` set (see `config.py`)
- `clients` table has `reg_at` integer/bigint column (nullable is OK)

Usage:
  python scripts/convert_joined_at_to_reg_at.py

Optional env vars:
  - REG_AT_UNIT: "s" (default) or "ms"
  - REG_AT_BATCH_SIZE: integer (default 500)
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure imports work even when run outside repo root.
# Adds the project root (parent of `scripts/`) to sys.path.
_PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config import CONFIG
from supabase_client import get_supabase_client


def _parse_joined_at(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None

    s = value.strip()
    if not s:
        return None

    # Common normalizations
    # - PostgREST often returns ISO with "T"
    # - Sometimes it may return space-separated timestamps (we wrote them like that)
    # - "Z" means UTC
    s = s.replace("Z", "+00:00")

    # Try ISO first
    try:
        dt = datetime.fromisoformat(s)
        return dt
    except Exception:
        pass

    # Try our stored format: "YYYY-MM-DD HH:MM:SS[.ffffff]"
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue

    return None


def _to_epoch_int(dt: datetime, unit: str) -> int:
    # joined_at is `timestamp without time zone`, so treat naive timestamps as UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    seconds = dt.timestamp()
    if unit == "ms":
        return int(round(seconds * 1000.0))
    return int(seconds)


def _get_batch_size() -> int:
    raw = os.getenv("REG_AT_BATCH_SIZE", "") or ""
    try:
        n = int(raw)
        return max(1, min(n, 1000))
    except Exception:
        return 500


def main() -> None:
    unit = (os.getenv("REG_AT_UNIT", "s") or "s").strip().lower()
    if unit not in ("s", "ms"):
        unit = "s"

    table = str(CONFIG.get("supabase_table_users", "clients"))
    sb = get_supabase_client()

    batch_size = _get_batch_size()

    total_updated = 0
    last_id = 0
    while True:
        # Keyset pagination by id so updates don't cause skipping.
        # Pull rows that are missing reg_at; filter joined_at in Python for robustness.
        resp = (
            sb.table(table)
            .select("id, joined_at, reg_at")
            .is_("reg_at", "null")
            .gt("id", last_id)
            .order("id")
            .limit(batch_size)
            .execute()
        )

        rows: List[Dict[str, Any]] = list(getattr(resp, "data", None) or [])
        if not rows:
            break

        updates: List[Dict[str, Any]] = []
        for r in rows:
            rid = r.get("id")
            dt = _parse_joined_at(r.get("joined_at"))
            if rid is None or dt is None:
                continue
            updates.append({"id": int(rid), "reg_at": _to_epoch_int(dt, unit=unit)})

        if updates:
            sb.table(table).upsert(updates, on_conflict="id").execute()
            total_updated += len(updates)

        # Advance cursor (regardless of how many were updated)
        try:
            last_id = int(rows[-1].get("id") or last_id)
        except Exception:
            last_id = last_id

    print(f"updated rows: {total_updated}")
    print("convert finished")


if __name__ == "__main__":
    main()

