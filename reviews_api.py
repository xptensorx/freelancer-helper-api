from typing import Any, Dict, List, Optional, Set, Tuple

from http_client import FreelancerApiClient


def fetch_reviews_for_user(
    client: FreelancerApiClient,
    *,
    to_user_id: int,
    limit: int = 100,
    compact: bool = True,
) -> Dict[str, Any]:
    """
    Fetch reviews for a freelancer user.

    No offset exists, so we set a large `limit` and keep params minimal.
    We only need `from_user_id` (reviewer) later.
    """
    params: List[Tuple[str, Any]] = [
        ("limit", limit),
        ("role", "freelancer"),
        ("to_users[]", int(to_user_id)),
        ("review_types[]", "contest"),
        ("review_types[]", "project"),
        ("order_by", "submit_date_desc"),
        # These tend to reduce payload / match web responses; can be removed if not needed.
        ("webapp", 1),
    ]
    if compact:
        params.append(("compact", "true"))
    return client.get("/projects/0.1/reviews/", params=params)


def extract_reviews(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = payload.get("result") if isinstance(payload, dict) else None
    if isinstance(result, dict) and isinstance(result.get("reviews"), list):
        return result["reviews"]
    if isinstance(payload.get("reviews"), list):
        return payload["reviews"]
    return []


def extract_reviewer_ids(payload: Dict[str, Any]) -> Set[int]:
    """
    Extract reviewer IDs (from_user_id) from a reviews payload.
    """
    reviewers: Set[int] = set()
    for r in extract_reviews(payload):
        if not isinstance(r, dict):
            continue
        from_uid = r.get("from_user_id")
        if from_uid is None:
            from_uid = r.get("from_user")  # fallback (some APIs embed objects)
        try:
            if isinstance(from_uid, dict):
                from_uid = from_uid.get("id") or from_uid.get("user_id")
            if from_uid is not None:
                reviewers.add(int(from_uid))
        except (TypeError, ValueError):
            continue
    return reviewers

