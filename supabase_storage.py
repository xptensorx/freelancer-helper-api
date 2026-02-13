from __future__ import annotations

from typing import Any, Dict, Iterable, List

from config import CONFIG
from supabase_client import get_supabase_client


_warned_missing = False


def upsert_users(user_rows: Iterable[Dict[str, Any]]) -> None:
    """
    Upsert reviewer user rows into Supabase.

    Expected shape per row (matches `public.clients` schema):
      {
        "id": <bigint>,
        "username": <text>,
        "display_name": <text>,
        "public_name": <text>,
        "location": <json>,
        "timezone": <json>,
        "joined_at": <timestamp>,
        "status": <json>
      }

    Table name comes from CONFIG['supabase_table_users'].
    """
    global _warned_missing

    rows = [r for r in user_rows if isinstance(r, dict) and r.get("id") is not None]
    if not rows:
        return

    try:
        sb = get_supabase_client()
    except Exception as e:
        # Don't crash the whole lead generation if Supabase isn't configured.
        if not _warned_missing:
            print(f"[supabase] disabled: {e}")
            _warned_missing = True
        return

    table = str(CONFIG.get("supabase_table_users", "clients"))
    # on_conflict ensures id-based upsert
    sb.table(table).upsert(rows, on_conflict="id").execute()

