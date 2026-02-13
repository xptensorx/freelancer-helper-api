from typing import Any, Dict, List, Set

from config import CONFIG
from http_client import FreelancerApiClient
from normalize import minimize_user, to_supabase_client_row
from reviews_api import extract_reviews, extract_reviewer_ids, fetch_reviews_for_user
from supabase_storage import upsert_users
from storage import JsonFileCache, append_jsonl, load_json, save_json_atomic
from users_api import (
    extract_user_id,
    extract_users,
    extract_users_map,
    fetch_directory_page,
    fetch_users_by_ids,
)


def chunked(items: List[int], size: int) -> List[List[int]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _freelancer_name(user_obj: Dict[str, Any]) -> str:
    return (
        user_obj.get("public_name")
        or user_obj.get("display_name")
        or user_obj.get("username")
        or ""
    )


def run_lead_generation() -> None:
    """
    Process:
    - Page through freelancer directory until the last user (offset pagination).
    - For each freelancer user_id:
      - fetch reviews (large limit, minimal params)
      - extract reviewer IDs (from_user_id)
      - fetch reviewer user objects (batched + cached)
      - write a JSONL lead record
    - Persist state after each user so you can safely resume.
    """

    state_path = str(CONFIG.get("state_path", "state.json"))
    leads_path = str(CONFIG.get("leads_output_path", "leads.jsonl"))
    cache_path = str(CONFIG.get("user_cache_path", "user_cache.json"))

    directory_limit = int(CONFIG.get("directory_limit", 20))
    reviews_limit = int(CONFIG.get("reviews_limit", 100))
    users_batch_size = int(CONFIG.get("users_batch_size", 50))

    state = load_json(
        state_path,
        default={
            "directory": {"offset": 0, "index_in_page": 0, "limit": directory_limit},
        },
    )
    directory_state = state.setdefault("directory", {})
    offset = int(directory_state.get("offset", 0))
    index_in_page = int(directory_state.get("index_in_page", 0))

    client = FreelancerApiClient.from_config()
    user_cache = JsonFileCache(cache_path)

    while True:
        payload = fetch_directory_page(
            client, limit=directory_limit, offset=offset, query="", compact=True
        )
        users = extract_users(payload)
        if not users:
            # reached last page
            break

        for idx, u in enumerate(users):
            if idx < index_in_page:
                continue

            freelancer_id = extract_user_id(u)
            if freelancer_id is None:
                # still advance progress to avoid getting stuck
                directory_state["index_in_page"] = idx + 1
                save_json_atomic(state_path, state)
                continue

            reviews_payload = fetch_reviews_for_user(
                client, to_user_id=freelancer_id, limit=reviews_limit, compact=True
            )
            review_count = len(extract_reviews(reviews_payload))
            reviewer_ids = sorted(extract_reviewer_ids(reviews_payload))

            # Fetch missing reviewer details with caching to reduce API calls
            missing = [rid for rid in reviewer_ids if user_cache.get(rid) is None]
            if missing:
                for batch in chunked(missing, users_batch_size):
                    users_payload = fetch_users_by_ids(client, batch, compact=True)
                    users_map = extract_users_map(users_payload)

                    supabase_rows = []
                    for uid, user_obj in users_map.items():
                        minimized = minimize_user(user_obj)
                        user_cache.set(uid, minimized)
                        supabase_rows.append(to_supabase_client_row(uid, minimized))

                    # Store to Supabase as soon as we have details
                    upsert_users(supabase_rows)
                user_cache.save()

            lead_record: Dict[str, Any] = {
                "freelancer_user_id": freelancer_id,
                "reviewer_ids": reviewer_ids,
                "reviewers": [
                    user_cache.get(rid) for rid in reviewer_ids if user_cache.get(rid)
                ],
            }
            append_jsonl(leads_path, lead_record)

            # Save progress after each user (safe resume)
            directory_state["offset"] = offset
            directory_state["index_in_page"] = idx + 1
            directory_state["limit"] = directory_limit
            save_json_atomic(state_path, state)

            print(
                f'ID: "{freelancer_id}" '
                f'Username: "{(u.get("username") or "")}" '
                f'Public Name: "{(u.get("public_name") or "")}" '
                f'Reviews: "{review_count}"'
            )

        # page completed
        offset += directory_limit
        index_in_page = 0
        directory_state["offset"] = offset
        directory_state["index_in_page"] = 0
        save_json_atomic(state_path, state)


if __name__ == "__main__":
    run_lead_generation()

