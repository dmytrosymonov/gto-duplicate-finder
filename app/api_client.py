"""GTO.UA API client with rate limiting and retry logic."""
import asyncio
import time
from typing import Any, Dict, List, Optional

import httpx

from app.config import BASE_URL, get_api_key
from app.rate_limiter import TokenBucketRateLimiter


class GTOApiError(Exception):
    """API request failed."""
    def __init__(self, message: str, status: Optional[int] = None, body: Any = None):
        super().__init__(message)
        self.status = status
        self.body = body


# Global rate limiter (shared across requests)
_rate_limiter: Optional[TokenBucketRateLimiter] = None
_request_count = 0
_response_times: List[float] = []


def get_rate_limiter(rps: float) -> TokenBucketRateLimiter:
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = TokenBucketRateLimiter(rate=rps)
    else:
        _rate_limiter.set_rate(rps)
    return _rate_limiter


def get_stats() -> Dict[str, Any]:
    """Return request stats for UI."""
    global _request_count, _response_times
    times = _response_times[-100:]
    avg_ms = sum(times) / len(times) * 1000 if times else 0
    peak_ms = max(times) * 1000 if times else 0
    return {
        "request_count": _request_count,
        "avg_response_ms": round(avg_ms, 1),
        "peak_response_ms": round(peak_ms, 1),
    }


def reset_stats() -> None:
    global _request_count, _response_times
    _request_count = 0
    _response_times.clear()


async def _request(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    rps: float = 5,
    max_retries: int = 3,
) -> Dict[str, Any]:
    """Execute request with rate limit and exponential backoff."""
    global _request_count, _response_times
    api_key = get_api_key()
    if not api_key:
        raise GTOApiError("API key not configured")

    if params is None:
        params = {}
    params["apikey"] = api_key

    limiter = get_rate_limiter(rps)
    last_exc = None

    for attempt in range(max_retries):
        await limiter.acquire()
        start = time.monotonic()
        try:
            resp = await client.request(
                method,
                f"{BASE_URL}{path}",
                params=params,
                timeout=30.0,
            )
            elapsed = time.monotonic() - start
            _request_count += 1
            _response_times.append(elapsed)
            if len(_response_times) > 500:
                _response_times.pop(0)

            if resp.status_code == 429:
                wait = 2 ** attempt
                await asyncio.sleep(wait)
                last_exc = GTOApiError("Rate limited (429)", status=429)
                continue

            if resp.status_code >= 500:
                wait = 2 ** attempt
                await asyncio.sleep(wait)
                last_exc = GTOApiError(
                    f"Server error {resp.status_code}",
                    status=resp.status_code,
                    body=resp.text[:500] if resp.text else None,
                )
                continue

            if resp.status_code >= 400:
                raise GTOApiError(
                    f"API error {resp.status_code}: {resp.text[:300]}",
                    status=resp.status_code,
                    body=resp.text[:500] if resp.text else None,
                )

            return resp.json()
        except httpx.TimeoutException as e:
            wait = 2 ** attempt
            await asyncio.sleep(wait)
            last_exc = GTOApiError(f"Request timeout: {e}")
        except httpx.RequestError as e:
            wait = 2 ** attempt
            await asyncio.sleep(wait)
            last_exc = GTOApiError(f"Request failed: {e}")

    raise last_exc or GTOApiError("Unknown error")


async def fetch_countries(lang: str = "en", rps: float = 5) -> List[Dict]:
    """GET /countries - returns list of countries (all pages)."""
    result: List[Dict] = []
    page = 1
    per_page = 500
    async with httpx.AsyncClient() as client:
        while True:
            data = await _request(
                client, "GET", "/countries",
                params={"lang": lang, "page": page, "per_page": per_page},
                rps=rps,
            )
            items = data.get("data", [])
            result.extend(items)
            if len(items) < per_page:
                break
            page += 1
    return result


async def fetch_cities(
    country_id: int,
    lang: str = "en",
    rps: float = 5,
) -> List[Dict]:
    """GET /cities - returns list of cities for country."""
    async with httpx.AsyncClient() as client:
        data = await _request(
            client, "GET", "/cities",
            params={"country_id": country_id, "lang": lang, "per_page": 1000},
            rps=rps,
        )
    return data.get("data", [])


async def fetch_hotels(
    city_id: int,
    country_id: Optional[int] = None,
    lang: str = "en",
    page: int = 1,
    per_page: int = 100,
    rps: float = 5,
) -> tuple[List[Dict], int]:
    """
    GET /hotels - returns hotels for city.
    Returns (hotels, total_count from info).
    """
    params: Dict[str, Any] = {
        "city_id": city_id,
        "lang": lang,
        "page": page,
        "per_page": per_page,
    }
    if country_id is not None:
        params["country_id"] = country_id

    async with httpx.AsyncClient() as client:
        data = await _request(client, "GET", "/hotels", params=params, rps=rps)

    hotels = data.get("data", [])
    return hotels, len(hotels)


async def fetch_hotel_info(
    hotel_id: int,
    lang: str = "en",
    rps: float = 5,
) -> Dict:
    """GET /hotel_info - returns extended hotel data (site, phone, etc.)."""
    async with httpx.AsyncClient() as client:
        data = await _request(
            client, "GET", "/hotel_info",
            params={"hotel_id": hotel_id, "lang": lang},
            rps=rps,
        )
    return data.get("data", {})
