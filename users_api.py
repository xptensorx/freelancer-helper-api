from typing import Any, Dict, Iterable, List, Optional, Tuple

from http_client import FreelancerApiClient


def fetch_directory_page(
    client: FreelancerApiClient,
    *,
    limit: int,
    offset: int,
    query: str = "",
    compact: bool = True,
) -> Dict[str, Any]:
    """
    Fetch one page from /users/0.1/users/directory/

    Keep params minimal to reduce payload and rate-limit risk.
    """
    params: List[Tuple[str, Any]] = [
        ("limit", limit),
        ("offset", offset),
        ("query", query),
    ]
    if compact:
        params.append(("compact", "true"))
    return client.get("/users/0.1/users/directory/", params=params)


def extract_users(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Be defensive about response shape
    result = payload.get("result") if isinstance(payload, dict) else None
    if isinstance(result, dict) and isinstance(result.get("users"), list):
        return result["users"]
    if isinstance(payload.get("users"), list):
        return payload["users"]
    return []


def extract_user_id(user_obj: Dict[str, Any]) -> Optional[int]:
    uid = user_obj.get("id")
    if uid is None:
        uid = user_obj.get("user_id")
    try:
        return int(uid) if uid is not None else None
    except (TypeError, ValueError):
        return None


def fetch_users_by_ids(
    client: FreelancerApiClient,
    user_ids: Iterable[int],
    *,
    compact: bool = True,
) -> Dict[str, Any]:
    """
    Batch fetch users by ID:
    /users/0.1/users?users[]=1&users[]=2...

    Keep params minimal; add more flags only if you truly need them.
    """
    params: List[Tuple[str, Any]] = []
    for uid in user_ids:
        params.append(("users[]", int(uid)))
    if compact:
        params.append(("compact", "true"))
    return client.get("/users/0.1/users", params=params)


def extract_users_map(payload: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    """
    Normalize response to a dict: {user_id: user_dict}
    """
    users = []
    result = payload.get("result") if isinstance(payload, dict) else None
    if isinstance(result, dict) and isinstance(result.get("users"), list):
        users = result["users"]
    elif isinstance(payload.get("users"), list):
        users = payload["users"]

    out: Dict[int, Dict[str, Any]] = {}
    for u in users:
        if not isinstance(u, dict):
            continue
        uid = extract_user_id(u)
        if uid is not None:
            out[uid] = u
    return out

