import requests
from typing import Any, Dict, Optional

from config import CONFIG

def get_headers() -> dict:
    """
    Freelancer API auth header.

    Note: Freelancer's OAuth v1 header is `freelancer-oauth-v1`.
    """
    token = CONFIG.get("oauth_access_token")
    if not token or token.startswith("<"):
        raise ValueError(
            "Missing CONFIG['oauth_access_token'] in config.py. "
            "Set it before calling the Freelancer API."
        )
    return {"freelancer-oauth-v1": token}


def api_get(path: str, *, params: Optional[Dict[str, Any]] = None, timeout_s: int = 30):
    """
    Convenience GET wrapper for Freelancer API.

    `path` can be like '/users/directory/' or 'users/directory/'.
    Returns the `requests.Response`.
    """
    base = CONFIG["api_base_url"].rstrip("/")
    path = path if path.startswith("/") else f"/{path}"
    url = f"{base}{path}"
    return requests.get(url, headers=get_headers(), params=params, timeout=timeout_s)
