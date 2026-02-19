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
) -> List[HotelRecord]:
    """Load all hotels with pagination."""
    page = 1
    per_page = 100
    all_hotels: List[HotelRecord] = []

    while True:
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
) -> List[DuplicatePair]:
    """Load hotels from multiple cities, enrich, run duplicate detection."""
    reset_stats()
    _clear_hotel_info_cache()

    def on_hotels(n: int, total: int) -> None:
        progress.hotels_loaded = n
        progress.progress_pct = min(40, 5 + int(35 * min(1, n / 500)))

    hotels: List[HotelRecord] = []
    for cid in city_ids:
        city_hotels = await load_all_hotels(cid, country_id, rps, on_progress=on_hotels)
        hotels.extend(city_hotels)
    progress.hotels_loaded = len(hotels)

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

    # Run duplicate detection
    progress._phase = "done"
    progress.progress_pct = 100
    pairs = find_duplicates(hotels)
    progress.flags_found = len(pairs)

    return pairs
