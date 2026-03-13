from typing import Any, Dict, List, Optional, Set, Tuple

from http_client import FreelancerApiClient


def fetch_reviews_for_user(
    client: FreelancerApiClient,
    *,
    to_user_id: int,
    limit: int = 100,
    compact: bool = True,
    offset_start: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Fetch one page of reviews for a freelancer user.

    Use fetch_all_reviews_for_user() to get up to max_reviews by paginating
    (for freelancers with 4000+ reviews). If the API does not support
    offset_start, only the first page is returned.
    """
    params: List[Tuple[str, Any]] = [
        ("limit", limit),
        ("role", "freelancer"),
        ("to_users[]", int(to_user_id)),
        ("review_types[]", "contest"),
        ("review_types[]", "project"),
        ("order_by", "submit_date_desc"),
        ("webapp", 1),
    ]
    if offset_start is not None:
        params.append(("offset_start", offset_start))
    if compact:
        params.append(("compact", "true"))
    return client.get("/projects/0.1/reviews/", params=params)


def fetch_all_reviews_for_user(
    client: FreelancerApiClient,
    *,
    to_user_id: int,
    max_reviews: int = 10000,
    page_size: int = 500,
    compact: bool = True,
) -> Dict[str, Any]:
    """
    Fetch reviews for a freelancer, paginating until we have up to max_reviews.

    Handles freelancers with 4000+ reviews by using offset_start (when supported
    by the API). If the API ignores offset_start, only the first page is returned.
    Returns a payload in the same shape as fetch_reviews_for_user so
    extract_reviews() / extract_reviewer_ids() work unchanged.
    """
    all_reviews: List[Dict[str, Any]] = []
    offset = 0
    max_pages = max(1, (max_reviews + page_size - 1) // page_size)

    for _ in range(max_pages):
        payload = fetch_reviews_for_user(
            client,
            to_user_id=to_user_id,
            limit=page_size,
            compact=compact,
            offset_start=offset,
        )
        page_reviews = extract_reviews(payload)
        if not page_reviews:
            break
        all_reviews.extend(page_reviews)
        if len(page_reviews) < page_size:
            break
        offset += len(page_reviews)
        if len(all_reviews) >= max_reviews:
            all_reviews = all_reviews[:max_reviews]
            break

    # Return payload shape expected by extract_reviews (result.reviews)
    return {"result": {"reviews": all_reviews}}


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

