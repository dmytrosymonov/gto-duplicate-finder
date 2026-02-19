"""Hotel duplicate detection: normalization, candidate generation, scoring."""
import math
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

# Stopwords for hotel names (lowercase)
NAME_STOPWORDS = {
    "hotel", "hostel", "resort", "spa", "apartments", "apartment", "suites",
    "suite", "inn", "villa", "villas", "lodge", "motel", "camp", "guesthouse",
    "guest", "house", "boutique", "design", "chain", "and", "&", "the", "a",
    "отель", "готель", "апартаменты", "апартаменти", "гостиница", "гост",
    "хостел", "резорт", "вилла", "пансион", "мотель",
}

# Address abbreviations
ADDR_ABBREV = {
    "street": "str", "str.": "str", "st.": "str", "st": "str",
    "avenue": "ave", "ave.": "ave", "ave": "ave",
    "boulevard": "blvd", "blvd.": "blvd", "blvd": "blvd",
    "road": "rd", "rd.": "rd", "rd": "rd",
    "lane": "ln", "ln.": "ln", "ln": "ln",
    "drive": "dr", "dr.": "dr", "dr": "dr",
    "place": "pl", "pl.": "pl", "pl": "pl",
}


def _remove_diacritics(s: str) -> str:
    nfd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def normalize_name(name: str) -> str:
    """Lowercase, remove punctuation, collapse spaces, remove stopwords."""
    if not name:
        return ""
    s = name.lower().strip()
    s = _remove_diacritics(s)
    s = re.sub(r"&", " and ", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    tokens = [t for t in s.split() if t not in NAME_STOPWORDS and len(t) >= 2]
    return " ".join(tokens)


def normalize_address(addr: str) -> str:
    """Lowercase, remove punctuation, collapse spaces, normalize abbreviations."""
    if not addr:
        return ""
    s = addr.lower().strip()
    s = _remove_diacritics(s)
    s = re.sub(r"[^\w\s.]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    tokens = []
    for t in s.split():
        tokens.append(ADDR_ABBREV.get(t, t))
    return " ".join(tokens)


def normalize_site(site: str) -> str:
    """Remove http/https, www, trailing slash."""
    if not site:
        return ""
    s = site.lower().strip()
    s = re.sub(r"^https?://", "", s)
    s = re.sub(r"^www\.", "", s)
    s = s.rstrip("/")
    s = s.split("/")[0]
    return s


def normalize_phone(phone: str) -> str:
    """Remove spaces, brackets, dashes; keep digits and leading +."""
    if not phone:
        return ""
    s = re.sub(r"[\s\-\(\)]", "", phone)
    digits = re.sub(r"\D", "", s)
    if s.startswith("+"):
        return "+" + digits
    return digits


def get_name_tokens(name: str, min_len: int = 3) -> Set[str]:
    """Token set from normalized name (tokens >= min_len)."""
    norm = normalize_name(name)
    return {t for t in norm.split() if len(t) >= min_len}


def haversine_km(
    lat1: float, lon1: float,
    lat2: float, lon2: float,
) -> float:
    """Distance in kilometers."""
    R = 6371
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def distance_score_m(distance_m: float) -> float:
    """1 at <50m, 0 at >2000m, smooth decay."""
    if distance_m < 50:
        return 1.0
    if distance_m > 2000:
        return 0.0
    return max(0, 1 - (distance_m - 50) / 1950)


def jaccard_tokens(a: Set[str], b: Set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def name_score(name1: str, name2: str) -> float:
    """Jaccard on name tokens + bonus for rare token match."""
    t1 = get_name_tokens(name1)
    t2 = get_name_tokens(name2)
    base = jaccard_tokens(t1, t2)
    if not t1 or not t2:
        return 0.0
    overlap = len(t1 & t2)
    rare_bonus = 0.1 * min(overlap, 3)
    return min(1.0, base + rare_bonus)


def address_score(addr1: str, addr2: str) -> float:
    """Share of matching key address tokens."""
    if not addr1 or not addr2:
        return 0.0
    n1 = normalize_address(addr1)
    n2 = normalize_address(addr2)
    t1 = set(n1.split())
    t2 = set(n2.split())
    return jaccard_tokens(t1, t2)


def contact_match(site1: str, site2: str, phone1: str, phone2: str) -> bool:
    """True if site or phone match (normalized)."""
    if site1 and site2:
        ns1 = normalize_site(site1)
        ns2 = normalize_site(site2)
        if ns1 and ns2 and ns1 == ns2:
            return True
    if phone1 and phone2:
        np1 = normalize_phone(phone1)
        np2 = normalize_phone(phone2)
        if np1 and np2 and np1 == np2:
            return True
    return False


@dataclass
class HotelRecord:
    id: int
    name: str
    address: str
    latitude: Optional[float]
    longitude: Optional[float]
    site: str = ""
    phone: str = ""
    city_id: int = 0
    country_id: int = 0

    @classmethod
    def from_api(cls, h: Dict[str, Any], extra: Optional[Dict] = None) -> "HotelRecord":
        lat = h.get("latitude")
        lon = h.get("longitude")
        if lat is not None:
            try:
                lat = float(lat)
            except (TypeError, ValueError):
                lat = None
        if lon is not None:
            try:
                lon = float(lon)
            except (TypeError, ValueError):
                lon = None

        rec = cls(
            id=int(h["id"]),
            name=h.get("name") or "",
            address=h.get("address") or "",
            latitude=lat,
            longitude=lon,
            city_id=int(h.get("city_id") or 0),
            country_id=int(h.get("country_id") or 0),
        )
        if extra:
            rec.site = (extra.get("site") or "").strip()
            rec.phone = (extra.get("phone") or "").strip()
        return rec


@dataclass
class DuplicatePair:
    hotel1: HotelRecord
    hotel2: HotelRecord
    confidence_score: float
    flag_type: str  # "auto" | "review"
    distance_m: Optional[float]
    name_score: float
    address_score_val: float
    contact_match_val: bool
    reason: str


def _candidate_radius(total_hotels: int) -> float:
    """250m default, 500m if city has <200 hotels."""
    return 500.0 if total_hotels < 200 else 250.0


def _candidates_geo(
    hotels: List[HotelRecord],
    radius_m: float,
) -> List[Tuple[HotelRecord, HotelRecord]]:
    """Pairs within radius (haversine)."""
    pairs = []
    with_coords = [(h, h.latitude, h.longitude) for h in hotels if h.latitude is not None and h.longitude is not None]
    for i, (h1, lat1, lon1) in enumerate(with_coords):
        for j, (h2, lat2, lon2) in enumerate(with_coords):
            if i >= j:
                continue
            km = haversine_km(lat1, lon1, lat2, lon2)
            if km * 1000 <= radius_m:
                pairs.append((h1, h2))
    return pairs


def _candidates_name_tokens(
    hotels: List[HotelRecord],
) -> List[Tuple[HotelRecord, HotelRecord]]:
    """Pairs with overlapping name tokens (min 1 token)."""
    token_to_hotels: Dict[frozenset, List[HotelRecord]] = {}
    for h in hotels:
        t = get_name_tokens(h.name)
        if not t:
            continue
        key = frozenset(t)
        if key not in token_to_hotels:
            token_to_hotels[key] = []
        token_to_hotels[key].append(h)

    pairs_set: Set[Tuple[int, int]] = set()
    for key, group in token_to_hotels.items():
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a, b = group[i].id, group[j].id
                pairs_set.add((min(a, b), max(a, b)))

    id_to_hotel = {h.id: h for h in hotels}
    return [
        (id_to_hotel[a], id_to_hotel[b])
        for a, b in pairs_set
        if a in id_to_hotel and b in id_to_hotel
    ]


def _candidates_no_coords(
    hotels: List[HotelRecord],
) -> List[Tuple[HotelRecord, HotelRecord]]:
    """For hotels without coords: use name+address overlap."""
    no_coords = [h for h in hotels if h.latitude is None or h.longitude is None]
    pairs = []
    for i in range(len(no_coords)):
        for j in range(i + 1, len(no_coords)):
            h1, h2 = no_coords[i], no_coords[j]
            t1 = get_name_tokens(h1.name)
            t2 = get_name_tokens(h2.name)
            if t1 & t2:
                pairs.append((h1, h2))
    return pairs


def _score_pair(
    h1: HotelRecord,
    h2: HotelRecord,
) -> Tuple[float, Optional[float], float, float, bool]:
    """
    Returns: (confidence, distance_m, name_score, address_score, contact_match).
    """
    dist_m = None
    if h1.latitude is not None and h1.longitude is not None and h2.latitude is not None and h2.longitude is not None:
        km = haversine_km(h1.latitude, h1.longitude, h2.latitude, h2.longitude)
        dist_m = km * 1000

    ns = name_score(h1.name, h2.name)
    as_ = address_score(h1.address, h2.address)
    cm = contact_match(h1.site, h2.site, h1.phone, h2.phone)

    dist_score = distance_score_m(dist_m) if dist_m is not None else 0.0

    # Weights: Contact 0.35, Distance 0.25, Name 0.25, Address 0.15
    contact_weight = 0.35 if (h1.site or h1.phone) and (h2.site or h2.phone) else 0.0
    if contact_weight == 0:
        total_rest = 0.25 + 0.25 + 0.15
        dist_w = 0.25 / total_rest
        name_w = 0.25 / total_rest
        addr_w = 0.15 / total_rest
    else:
        dist_w = 0.25
        name_w = 0.25
        addr_w = 0.15

    contact_val = 1.0 if cm else 0.0
    confidence = (
        contact_weight * contact_val +
        dist_w * dist_score +
        name_w * ns +
        addr_w * as_
    )
    if contact_weight == 0:
        confidence = dist_w * dist_score + name_w * ns + addr_w * as_

    # Brand protection: high name, low address, high distance -> downgrade
    if ns >= 0.85 and as_ < 0.3 and dist_m is not None and dist_m > 500:
        confidence *= 0.7

    return confidence, dist_m, ns, as_, cm


def _flag_type(
    confidence: float,
    dist_m: Optional[float],
    ns: float,
    cm: bool,
) -> str:
    """Return 'auto', 'review', or '' (no flag)."""
    if confidence < 0.60:
        return ""
    if dist_m is not None and dist_m > 2000 and not cm:
        return ""

    # Auto: (contact match AND name >= 0.75) OR (dist < 150m AND name >= 0.88)
    if cm and ns >= 0.75:
        return "auto"
    if dist_m is not None and dist_m < 150 and ns >= 0.88:
        return "auto"

    # Review: confidence >= 0.75 without strong contact
    if confidence >= 0.75:
        return "review"

    return ""


def _reason(dist_m: Optional[float], ns: float, as_: float, cm: bool) -> str:
    parts = []
    if dist_m is not None:
        parts.append(f"расстояние {int(dist_m)} м")
    parts.append(f"схожесть названия {ns:.2f}")
    parts.append(f"схожесть адреса {as_:.2f}")
    if cm:
        parts.append("совпадение телефона/сайта")
    return ", ".join(parts)


def find_duplicates(
    hotels: List[HotelRecord],
) -> List[DuplicatePair]:
    """Main entry: generate candidates, score, flag."""
    total = len(hotels)
    radius = _candidate_radius(total)

    pairs_set: Set[Tuple[int, int]] = set()
    for h1, h2 in _candidates_geo(hotels, radius):
        a, b = min(h1.id, h2.id), max(h1.id, h2.id)
        pairs_set.add((a, b))

    for h1, h2 in _candidates_name_tokens(hotels):
        a, b = min(h1.id, h2.id), max(h1.id, h2.id)
        pairs_set.add((a, b))

    for h1, h2 in _candidates_no_coords(hotels):
        a, b = min(h1.id, h2.id), max(h1.id, h2.id)
        pairs_set.add((a, b))

    id_to_hotel = {h.id: h for h in hotels}
    results: List[DuplicatePair] = []

    for a, b in pairs_set:
        h1 = id_to_hotel[a]
        h2 = id_to_hotel[b]
        confidence, dist_m, ns, as_, cm = _score_pair(h1, h2)
        flag = _flag_type(confidence, dist_m, ns, cm)
        if not flag:
            continue
        reason = _reason(dist_m, ns, as_, cm)
        results.append(DuplicatePair(
            hotel1=h1,
            hotel2=h2,
            confidence_score=confidence,
            flag_type=flag,
            distance_m=dist_m,
            name_score=ns,
            address_score_val=as_,
            contact_match_val=cm,
            reason=reason,
        ))

    return results
