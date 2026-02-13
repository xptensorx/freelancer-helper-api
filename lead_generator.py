import requests

from config import CONFIG
from oauth import api_get, get_headers


def fetch_freelancer_directory_users(
    *,
    limit: int = 20,
    offset: int = 0,
    query: str = "",
    avatar: bool = True,
    country_details: bool = True,
    display_info: bool = True,
    job_ranks: bool = True,
    jobs: bool = True,
    location_details: bool = True,
    online_offline_details: bool = True,
    preferred_details: bool = True,
    profile_description: bool = True,
    pool_details: bool = True,
    qualification_details: bool = True,
    reputation: bool = True,
    rising_star: bool = True,
    status: bool = True,
    webapp: int = 1,
    compact: bool = True,
    new_errors: bool = True,
    new_pools: bool = True,
):
    """
    Fetch users from Freelancer directory endpoint.

    Endpoint (from your example):
    /users/directory/?limit=20&offset=0&query=...&...
    """

    def b(v: bool) -> str:
        # API expects true/false strings in query params
        return "true" if v else "false"

    params = {
        "limit": limit,
        "offset": offset,
        "query": query,
        "avatar": b(avatar),
        "country_details": b(country_details),
        "display_info": b(display_info),
        "job_ranks": b(job_ranks),
        "jobs": b(jobs),
        "location_details": b(location_details),
        "online_offline_details": b(online_offline_details),
        "preferred_details": b(preferred_details),
        "profile_description": b(profile_description),
        "pool_details": b(pool_details),
        "qualification_details": b(qualification_details),
        "reputation": b(reputation),
        "rising_star": b(rising_star),
        "status": b(status),
        "webapp": webapp,
        "compact": b(compact),
        "new_errors": b(new_errors),
        "new_pools": b(new_pools),
    }

    resp = api_get("/users/directory/", params=params)
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    # Simple smoke run: fetch first page and print basic identifiers
    data = fetch_freelancer_directory_users(limit=20, offset=0, query="")

    users = data.get("result", {}).get("users") or data.get("users") or []
    print(f"Fetched {len(users)} users")

    for u in users[:20]:
        uid = u.get("id") or u.get("user_id")
        username = u.get("username") or u.get("public_name") or u.get("display_name")

        print(f"- {uid} | {username}")

