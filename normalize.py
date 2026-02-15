from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _get(d: Any, *path: str) -> Any:
    cur = d
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    return cur


def minimize_user(user_obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep only the fields you said you need.

    Output shape (example):
    {
      "username": "...",
      "closed": false,
      "registration_date": 1458235929,
      "display_name": "...",
      "location": {"country": {"name": "United States"}, "city": "Coral Gables"},
      "status": {"email_verified": true},
      "public_name": "Juan A.",
      "timezone": {"id": 120, "country": "US", "timezone": "...", "offset": 19.0},
      "registration_completed": true
    }
    """

    country_name = _get(user_obj, "location", "country", "name")
    city = _get(user_obj, "location", "city")
    status_obj = _get(user_obj, "status")
    tz = _get(user_obj, "timezone")

    out: Dict[str, Any] = {
        "username": user_obj.get("username"),
        "closed": user_obj.get("closed"),
        "registration_date": user_obj.get("registration_date"),
        "display_name": user_obj.get("display_name"),
        # Store as { country: "<name>", city: "<city>" }
        "location": {
            "country": country_name,
            "city": city,
        },
        # Store complete status object from API (not just email_verified)
        "status": status_obj if isinstance(status_obj, dict) else None,
        "public_name": user_obj.get("public_name"),
        "timezone": tz if isinstance(tz, dict) else None,
        "registration_completed": user_obj.get("registration_completed"),
    }

    # Drop None-only containers to keep JSON clean
    if out["location"].get("country") is None and out["location"].get("city") is None:
        out["location"] = None
    if out["status"] is None:
        out.pop("status", None)
    if out["timezone"] is None:
        out.pop("timezone", None)
    if out.get("location") is None:
        out.pop("location", None)

    return out


def to_supabase_client_row(user_id: int, minimized_user: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map our minimized user payload into your Supabase `public.clients` schema.

    Schema requires NOT NULL:
    - username, display_name, public_name
    - location (json), timezone (json), status (json)
    - joined_at (timestamp without time zone) [nullable in latest schema is OK too]
    - reg_at (integer) optional (epoch seconds)

    We also explicitly set `id` = Freelancer user_id so upserts are stable.
    """

    username = (minimized_user.get("username") or "") if isinstance(minimized_user, dict) else ""
    display_name = (minimized_user.get("display_name") or "") if isinstance(minimized_user, dict) else ""
    public_name = (minimized_user.get("public_name") or "") if isinstance(minimized_user, dict) else ""

    # Fallbacks to satisfy NOT NULL
    if not display_name:
        display_name = username or str(user_id)
    if not public_name:
        public_name = display_name
    if not username:
        username = display_name

    location = minimized_user.get("location") if isinstance(minimized_user, dict) else None
    timezone_obj = minimized_user.get("timezone") if isinstance(minimized_user, dict) else None
    status = minimized_user.get("status") if isinstance(minimized_user, dict) else None
    if not isinstance(location, dict):
        location = {}
    if not isinstance(timezone_obj, dict):
        timezone_obj = {}
    if not isinstance(status, dict):
        status = {}

    reg_ts = minimized_user.get("registration_date") if isinstance(minimized_user, dict) else None
    reg_at: Optional[int] = None
    joined_at = None
    try:
        if reg_ts is not None:
            reg_at = int(reg_ts)
            joined_at = datetime.fromtimestamp(reg_at, tz=timezone.utc).replace(tzinfo=None)
    except Exception:
        joined_at = None
        reg_at = None
    if joined_at is None:
        # last-resort: "now" as naive UTC
        joined_at = datetime.now(tz=timezone.utc).replace(tzinfo=None)

    row: Dict[str, Any] = {
        "id": int(user_id),
        "username": username,
        "display_name": display_name,
        "public_name": public_name,
        # Ensure stored format: { country: <string>, city: <string> }
        "location": {
            "country": location.get("country"),
            "city": location.get("city"),
        },
        "timezone": timezone_obj,
        "joined_at": joined_at.isoformat(sep=" "),
        "status": status,
    }

    # From now on, store registration time as integer too (epoch seconds).
    # Only set it when we have a clean value.
    if reg_at is not None:
        row["reg_at"] = reg_at

    return row

