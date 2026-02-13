import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import requests

from config import CONFIG
from oauth import get_headers
from rate_limiter import RateLimiter


ParamsType = Union[Dict[str, Any], List[Tuple[str, Any]]]


@dataclass
class FreelancerApiClient:
    """
    Rate-limited, retrying HTTP client for Freelancer API.

    Handles:
    - global delay (rate limiting)
    - retries with exponential backoff on 429 / transient 5xx / network timeouts
    """

    api_root_url: str
    limiter: RateLimiter
    timeout_s: int = 30
    max_retries: int = 6
    backoff_base_s: float = 1.0
    backoff_max_s: float = 60.0

    def __post_init__(self) -> None:
        self.api_root_url = self.api_root_url.rstrip("/")
        self.session = requests.Session()

    @staticmethod
    def _derive_api_root_from_legacy(api_base_url: str) -> str:
        # Example legacy: https://www.freelancer.com/api/users/0.1 -> https://www.freelancer.com/api
        api_base_url = api_base_url.rstrip("/")
        suffix = "/users/0.1"
        if api_base_url.endswith(suffix):
            return api_base_url[: -len(suffix)]
        # best-effort fallback
        if "/api/" in api_base_url:
            return api_base_url.split("/api/")[0] + "/api"
        return api_base_url

    @classmethod
    def from_config(cls) -> "FreelancerApiClient":
        api_root = CONFIG.get("api_root_url")
        if not api_root:
            api_root = cls._derive_api_root_from_legacy(CONFIG["api_base_url"])

        limiter = RateLimiter(
            min_interval_s=float(CONFIG.get("request_min_interval_s", 0.8)),
            requests_per_minute=CONFIG.get("requests_per_minute", 50),
            jitter_s=float(CONFIG.get("request_jitter_s", 0.2)),
        )
        return cls(
            api_root_url=str(api_root),
            limiter=limiter,
            timeout_s=int(CONFIG.get("timeout_s", 30)),
            max_retries=int(CONFIG.get("max_retries", 6)),
            backoff_base_s=float(CONFIG.get("backoff_base_s", 1.0)),
            backoff_max_s=float(CONFIG.get("backoff_max_s", 60.0)),
        )

    def get(self, path: str, *, params: Optional[ParamsType] = None) -> Dict[str, Any]:
        url = path if path.startswith("http") else f"{self.api_root_url}{path}"
        return self._request_json("GET", url, params=params)

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        params: Optional[ParamsType] = None,
    ) -> Dict[str, Any]:
        last_exc: Optional[BaseException] = None
        last_status: Optional[int] = None
        last_text: Optional[str] = None

        for attempt in range(self.max_retries + 1):
            self.limiter.wait()
            try:
                resp = self.session.request(
                    method,
                    url,
                    headers=get_headers(),
                    params=params,
                    timeout=self.timeout_s,
                )

                last_status = resp.status_code
                # keep only a small snippet to avoid huge logs
                try:
                    last_text = (resp.text or "")[:500]
                except Exception:
                    last_text = None

                if resp.status_code in (429, 500, 502, 503, 504):
                    # Rate limit or transient failure
                    sleep_s = self._compute_retry_sleep(resp, attempt)
                    time.sleep(sleep_s)
                    continue

                resp.raise_for_status()
                return resp.json()

            except (requests.Timeout, requests.ConnectionError) as e:
                last_exc = e
                time.sleep(self._backoff(attempt))
                continue
            except requests.HTTPError as e:
                # Non-retryable HTTP error
                raise
            except ValueError:
                # JSON decode failed, return text-ish wrapper
                return {"ok": False, "raw": getattr(resp, "text", None), "url": url}

        if last_exc:
            raise last_exc
        raise RuntimeError(
            f"Request failed after retries: {method} {url} "
            f"(last_status={last_status}, last_body={last_text!r})"
        )

    def _compute_retry_sleep(self, resp: requests.Response, attempt: int) -> float:
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                return min(self.backoff_max_s, float(retry_after))
            except ValueError:
                pass
        return self._backoff(attempt)

    def _backoff(self, attempt: int) -> float:
        # exponential backoff capped
        return min(self.backoff_max_s, self.backoff_base_s * (2 ** attempt))

