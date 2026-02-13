from __future__ import annotations

from typing import Optional

from config import CONFIG


_client = None


def get_supabase_client():
    """
    Lazy-init Supabase client.

    Requires:
    - CONFIG['supabase_url']
    - CONFIG['supabase_service_role_key']
    """
    global _client
    if _client is not None:
        return _client

    url = str(CONFIG.get("supabase_url", "") or "")
    key = str(CONFIG.get("supabase_service_role_key", "") or "")
    if not url or url.startswith("<") or not key or key.startswith("<"):
        raise ValueError(
            "Supabase credentials missing. Set SUPABASE_URL and "
            "SUPABASE_SERVICE_ROLE_KEY in your environment (or config.py)."
        )

    # Import only when needed so the project can run without Supabase installed.
    from supabase import create_client  # type: ignore

    _client = create_client(url, key)
    return _client

