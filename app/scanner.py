"""Orchestrates hotel loading, hotel_info enrichment, and duplicate detection."""
import asyncio
from typing import Any, Callable, Dict, List, Optional

from app.api_client import (
    fetch_hotel_info,
    fetch_hotels,
    get_rate_limiter,
    get_stats,
    reset_stats,
)
from app.deduplication import DuplicatePair, HotelRecord, find_duplicates


def _text_contains_error(obj: Any) -> bool:
    """Check if any string in obj (recursively) contains 'error' case-insensitive."""
    if isinstance(obj, str):
        return "error" in obj.lower()
    if isinstance(obj, dict):
        return any(_text_contains_error(v) for v in obj.values())
    if isinstance(obj, (list, tuple)):
        return any(_text_contains_error(x) for x in obj)
    return False


_DESCRIPTION_KEYS = (
    "description", "description_ru", "description_en", "description_uk",
    "short_description", "long_description", "text", "content",
)


def _extract_description(info: Dict[str, Any]) -> str:
    """Extract description text from hotel_info API response."""
    parts = []
    for key in _DESCRIPTION_KEYS:
        val = info.get(key)
        if isinstance(val, str) and val.strip():
            parts.append(val.strip())
    if not parts:
        for k, v in info.items():
            if isinstance(v, str) and len(v) > 50 and "error" in v.lower():
                parts.append(v)
    return " | ".join(parts) if parts else ""


_STAR_KEYS = ("stars", "star_rating", "rating", "hotel_stars", "category", "star")


def _extract_stars(info: Dict[str, Any]) -> str:
    """Extract star rating from hotel_info API response."""
    for key in _STAR_KEYS:
        val = info.get(key)
        if val is not None:
            if isinstance(val, (int, float)):
                return str(int(val)) if val == int(val) else str(val)
            if isinstance(val, str) and val.strip():
                return val.strip()
    return ""


class ScanProgress:
    def __init__(self):
        self.hotels_loaded = 0
        self.comparisons_done = 0
        self.flags_found = 0
        self.error: Optional[str] = None
        self.progress_pct = 0
        self._total_to_enrich = 0
        self._phase = "loading"  # loading | enriching | done


async def load_all_hotels(
    city_id: int,
    country_id: Optional[int],
    rps: float,
    on_progress: Optional[Callable[[int, int], None]] = None,
    check_cancel: Optional[Callable[[], bool]] = None,
) -> List[HotelRecord]:
    """Load all hotels with pagination."""
    page = 1
    per_page = 100
    all_hotels: List[HotelRecord] = []

    while True:
        if check_cancel and check_cancel():
            break
        hotels_data, _ = await fetch_hotels(
            city_id=city_id,
            country_id=country_id,
            page=page,
            per_page=per_page,
            rps=rps,
        )
        for h in hotels_data:
            all_hotels.append(HotelRecord.from_api(h))

        if on_progress:
            on_progress(len(all_hotels), len(all_hotels))

        if len(hotels_data) < per_page:
            break
        page += 1

    return all_hotels


_hotel_info_cache: Dict[int, Dict[str, Any]] = {}


def _clear_hotel_info_cache() -> None:
    global _hotel_info_cache
    _hotel_info_cache.clear()


async def _get_hotel_info(hotel_id: int, rps: float) -> Dict[str, Any]:
    if hotel_id in _hotel_info_cache:
        return _hotel_info_cache[hotel_id]
    try:
        data = await fetch_hotel_info(hotel_id, rps=rps)
        _hotel_info_cache[hotel_id] = data
        return data
    except Exception:
        return {}


def _needs_info_for_pair(h1: HotelRecord, h2: HotelRecord) -> bool:
    """Fetch hotel_info only for pairs that might benefit (borderline/top candidates)."""
    from app.deduplication import address_score, haversine_km, name_score
    ns = name_score(h1.name, h2.name)
    if ns < 0.5:
        return False
    dist_ok = False
    if h1.latitude and h1.longitude and h2.latitude and h2.longitude:
        km = haversine_km(h1.latitude, h1.longitude, h2.latitude, h2.longitude)
        if km * 1000 <= 500:
            dist_ok = True
    as_ = address_score(h1.address, h2.address)
    if dist_ok or as_ > 0.2 or ns >= 0.75:
        return True
    return False


async def run_scan(
    city_ids: List[int],
    country_id: Optional[int],
    rps: float,
    progress: ScanProgress,
    check_cancel: Optional[Callable[[], bool]] = None,
) -> List[DuplicatePair]:
    """Load hotels from multiple cities, enrich, run duplicate detection."""
    reset_stats()
    _clear_hotel_info_cache()

    def on_hotels(n: int, total: int) -> None:
        progress.hotels_loaded = n
        progress.progress_pct = min(40, 5 + int(35 * min(1, n / 500)))

    hotels: List[HotelRecord] = []
    for cid in city_ids:
        if check_cancel and check_cancel():
            break
        city_hotels = await load_all_hotels(
            cid, country_id, rps, on_progress=on_hotels, check_cancel=check_cancel
        )
        hotels.extend(city_hotels)
    progress.hotels_loaded = len(hotels)

    if check_cancel and check_cancel():
        progress._phase = "done"
        progress.progress_pct = 100
        return find_duplicates(hotels)

    # Enrich with hotel_info for hotels that might be in duplicate pairs
    # Build initial candidate set (geo + name overlap)
    from app.deduplication import (
        _candidates_geo,
        _candidates_name_tokens,
        _candidate_radius,
    )
    radius = _candidate_radius(len(hotels))
    cand_pairs = _candidates_geo(hotels, radius) + _candidates_name_tokens(hotels)
    ids_to_enrich: set[int] = set()
    for h1, h2 in cand_pairs:
        if _needs_info_for_pair(h1, h2):
            ids_to_enrich.add(h1.id)
            ids_to_enrich.add(h2.id)

    progress._total_to_enrich = len(ids_to_enrich)
    progress._phase = "enriching"
    progress.progress_pct = 40

    # Fetch hotel_info for candidates (rate limited)
    for i, hid in enumerate(ids_to_enrich):
        if check_cancel and check_cancel():
            break
        if (i + 1) % 10 == 0 or i == 0:
            progress.comparisons_done = i + 1
            if progress._total_to_enrich > 0:
                progress.progress_pct = min(90, 40 + int(50 * (i + 1) / progress._total_to_enrich))
        info = await _get_hotel_info(hid, rps)
        rec = next((h for h in hotels if h.id == hid), None)
        if rec and info:
            rec.site = (info.get("site") or "").strip()
            rec.phone = (info.get("phone") or "").strip()

    progress.comparisons_done = len(ids_to_enrich)
    progress._phase = "done"
    progress.progress_pct = 100
    pairs = find_duplicates(hotels)
    progress.flags_found = len(pairs)

    return pairs


async def run_error_scan(
    city_ids: List[int],
    country_id: Optional[int],
    rps: float,
    progress: ScanProgress,
    check_cancel: Optional[Callable[[], bool]] = None,
) -> List[Dict[str, Any]]:
    """Load hotels, fetch hotel_info, return hotels with 'Error' in any text field."""
    reset_stats()
    _clear_hotel_info_cache()

    def on_hotels(n: int, total: int) -> None:
        progress.hotels_loaded = n
        progress.progress_pct = min(40, 5 + int(35 * min(1, n / 500)))

    hotels: List[HotelRecord] = []
    for cid in city_ids:
        if check_cancel and check_cancel():
            break
        city_hotels = await load_all_hotels(
            cid, country_id, rps, on_progress=on_hotels, check_cancel=check_cancel
        )
        hotels.extend(city_hotels)
    progress.hotels_loaded = len(hotels)
    progress._phase = "enriching"
    progress.progress_pct = 40

    bad: List[Dict[str, Any]] = []
    total = len(hotels)
    for i, h in enumerate(hotels):
        if check_cancel and check_cancel():
            break
        if (i + 1) % 20 == 0 or i == 0:
            progress.comparisons_done = i + 1
            if total > 0:
                progress.progress_pct = min(95, 40 + int(55 * (i + 1) / total))
        info = await _get_hotel_info(h.id, rps=rps)
        if info and _text_contains_error(info):
            bad.append({
                "hotel_id": h.id,
                "name": h.name or "",
                "address": h.address or "",
                "stars": _extract_stars(info),
                "reason": "Contains 'Error' in description",
            })

    progress.comparisons_done = i + 1
    progress._phase = "done"
    progress.progress_pct = 100
    progress.flags_found = len(bad)
    return bad
