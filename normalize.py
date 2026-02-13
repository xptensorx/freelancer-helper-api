from __future__ import annotations

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
    email_verified = _get(user_obj, "status", "email_verified")
    tz = _get(user_obj, "timezone")

    out: Dict[str, Any] = {
        "username": user_obj.get("username"),
        "closed": user_obj.get("closed"),
        "registration_date": user_obj.get("registration_date"),
        "display_name": user_obj.get("display_name"),
        "location": {
            "country": {"name": country_name} if country_name is not None else None,
            "city": city,
        },
        "status": {"email_verified": email_verified} if email_verified is not None else None,
        "public_name": user_obj.get("public_name"),
        "timezone": tz if isinstance(tz, dict) else None,
        "registration_completed": user_obj.get("registration_completed"),
    }

    # Drop None-only containers to keep JSON clean
    if out["location"]["country"] is None and out["location"]["city"] is None:
        out["location"] = None
    if out["status"] is None:
        out.pop("status", None)
    if out["timezone"] is None:
        out.pop("timezone", None)
    if out.get("location") is None:
        out.pop("location", None)

    return out

