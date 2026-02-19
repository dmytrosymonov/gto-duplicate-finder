"""FastAPI application - web UI and API endpoints."""
import asyncio
import io
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from openpyxl import Workbook
from openpyxl.styles import Font
from jinja2 import Environment, FileSystemLoader

from app.api_client import fetch_cities, fetch_countries, get_stats
from app.config import DEFAULT_RPS, get_api_key, set_api_key
from app.deduplication import DuplicatePair
from app.scanner import ScanProgress, run_scan

app = FastAPI(title="GTO Hotel Duplicate Finder")

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))

# Scan state: scan_id -> {scan_id, progress, results, done, error}
_scans: Dict[str, Dict[str, Any]] = {}
_MAX_SCANS = 50


def _render(name: str, **kwargs: Any) -> str:
    t = env.get_template(name)
    return t.render(**kwargs)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> str:
    return _render("index.html", api_key_set=bool(get_api_key()))


@app.post("/api/apikey")
async def api_set_apikey(request: Request) -> Dict[str, str]:
    body = await request.json()
    key = (body.get("apikey") or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="apikey required")
    set_api_key(key)
    return {"status": "ok"}


@app.get("/api/countries")
async def api_countries(request: Request) -> Dict[str, Any]:
    rps = float(request.query_params.get("rps", DEFAULT_RPS))
    try:
        data = await fetch_countries(rps=rps)
        items = [{"id": c.get("id"), "name": c.get("name") or ""} for c in data]
        items.sort(key=lambda x: (x["name"] or "").strip().lower())
        return {"data": items}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/api/cities")
async def api_cities(request: Request) -> Dict[str, Any]:
    country_id = request.query_params.get("country_id")
    if not country_id:
        raise HTTPException(status_code=400, detail="country_id required")
    rps = float(request.query_params.get("rps", DEFAULT_RPS))
    try:
        data = await fetch_cities(int(country_id), rps=rps)
        items = [{"id": c.get("id"), "name": c.get("name") or ""} for c in data]
        items.sort(key=lambda x: (x["name"] or "").strip().lower())
        return {"data": items}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


def _prune_scans() -> None:
    global _scans
    while len(_scans) >= _MAX_SCANS:
        done_ids = [sid for sid in list(_scans.keys()) if _scans[sid].get("done")]
        remove_id = done_ids[0] if done_ids else next(iter(_scans.keys()))
        del _scans[remove_id]


@app.post("/api/scan")
async def api_scan_start(request: Request) -> Dict[str, Any]:
    global _scans
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    city_ids = body.get("city_ids")
    if city_ids and isinstance(city_ids, list):
        city_ids = [int(x) for x in city_ids if x is not None and str(x).strip() != ""]
    elif body.get("city_id") is not None:
        try:
            city_ids = [int(body["city_id"])]
        except (TypeError, ValueError):
            city_ids = []
    else:
        city_ids = []
    if not city_ids:
        raise HTTPException(
            status_code=400,
            detail="city_ids or city_id required (received: %s)" % (list(body.keys()) if body else "empty body"),
        )
    country_id = body.get("country_id")
    rps = float(body.get("rps", DEFAULT_RPS))
    if rps <= 0 or rps > 100:
        rps = DEFAULT_RPS

    if not get_api_key():
        raise HTTPException(status_code=400, detail="API key not configured")

    _prune_scans()
    scan_id = str(uuid.uuid4())
    progress = ScanProgress()
    _scans[scan_id] = {
        "scan_id": scan_id,
        "progress": progress,
        "results": None,
        "done": False,
        "error": None,
    }

    async def _run() -> None:
        try:
            results = await run_scan(
                city_ids=city_ids,
                country_id=int(country_id) if country_id is not None else None,
                rps=rps,
                progress=progress,
            )
            if scan_id in _scans:
                _scans[scan_id]["results"] = results
                _scans[scan_id]["done"] = True
        except Exception as e:
            if scan_id in _scans:
                _scans[scan_id]["error"] = str(e)
                _scans[scan_id]["done"] = True
                _scans[scan_id]["progress"].error = str(e)

    asyncio.create_task(_run())

    return {"scan_id": scan_id, "status": "started"}


@app.get("/api/scan/status")
async def api_scan_status(request: Request) -> Dict[str, Any]:
    global _scans
    stats = get_stats()
    scan_id = request.query_params.get("scan_id")
    scan = _scans.get(scan_id) if scan_id else (list(_scans.values())[-1] if _scans else None)

    if not scan:
        return {
            "active": False,
            "done": True,
            "hotels_loaded": 0,
            "comparisons_done": 0,
            "flags_found": 0,
            "results": [],
            "error": None,
            "stats": stats,
            "progress_pct": 0,
        }

    p = scan["progress"]
    resp = {
        "active": not scan.get("done"),
        "done": scan.get("done", False),
        "hotels_loaded": p.hotels_loaded,
        "comparisons_done": p.comparisons_done,
        "flags_found": p.flags_found,
        "results": None,
        "error": scan.get("error") or p.error,
        "stats": stats,
        "progress_pct": getattr(p, "progress_pct", 100 if scan.get("done") else 0),
    }

    if scan.get("done") and scan.get("results") is not None:
        resp["results"] = _pairs_to_rows(scan["results"])

    return resp


def _union_find_parent(parent: dict, x: int) -> int:
    if parent[x] != x:
        parent[x] = _union_find_parent(parent, parent[x])
    return parent[x]


def _pairs_to_rows(pairs: list) -> list:
    """Merge duplicate pairs into clusters, return table rows."""
    from app.deduplication import HotelRecord
    if not pairs:
        return []
    id_to_hotel = {}
    pair_by_edge = {}
    for p in pairs:
        id_to_hotel[p.hotel1.id] = p.hotel1
        id_to_hotel[p.hotel2.id] = p.hotel2
        a, b = min(p.hotel1.id, p.hotel2.id), max(p.hotel1.id, p.hotel2.id)
        pair_by_edge[(a, b)] = p
    all_ids = list(id_to_hotel.keys())
    parent = {i: i for i in all_ids}
    for p in pairs:
        a, b = p.hotel1.id, p.hotel2.id
        pa, pb = _union_find_parent(parent, a), _union_find_parent(parent, b)
        if pa != pb:
            parent[pa] = pb
    clusters: dict[int, list[int]] = {}
    for i in all_ids:
        root = _union_find_parent(parent, i)
        clusters.setdefault(root, []).append(i)
    rows = []
    for cluster_ids in clusters.values():
        cluster_ids = sorted(set(cluster_ids))
        if len(cluster_ids) < 2:
            continue
        hotels = [id_to_hotel[i] for i in cluster_ids]
        names = list(dict.fromkeys(h.name for h in hotels if h.name))
        addr_parts = list(dict.fromkeys(h.address for h in hotels if h.address))
        reason = "Группа из %d отелей" % len(cluster_ids)
        flag_type = "review"
        score = 0.0
        for i in range(len(cluster_ids)):
            for j in range(i + 1, len(cluster_ids)):
                a, b = cluster_ids[i], cluster_ids[j]
                p = pair_by_edge.get((min(a, b), max(a, b)))
                if p and (p.confidence_score > score or reason == "Группа из %d отелей" % len(cluster_ids)):
                    reason = p.reason
                    flag_type = p.flag_type
                    score = p.confidence_score
        rows.append({
            "hotel_name": " / ".join(names) if names else "",
            "id1": cluster_ids[0],
            "id2": cluster_ids[1:],
            "address": " | ".join(addr_parts) if addr_parts else "",
            "reason": reason,
            "flag_type": flag_type,
            "confidence_score": round(score, 3),
        })
    return rows


@app.post("/api/export/excel")
async def api_export_excel(request: Request) -> StreamingResponse:
    """Export results to Excel file."""
    try:
        body = await request.json() or {}
    except Exception:
        body = {}
    results = body.get("results") or body.get("data") or []
    if not isinstance(results, list):
        raise HTTPException(status_code=400, detail="results array required")

    wb = Workbook()
    ws = wb.active
    ws.title = "Дубликаты"
    headers = ["Название отеля", "ID 1", "ID 2", "Адрес", "Общий скоринг", "Причина флага"]
    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.font = Font(bold=True)
    for row_idx, r in enumerate(results, 2):
        id2_val = r.get("id2")
        id2_str = ", ".join(str(x) for x in id2_val) if isinstance(id2_val, list) else str(id2_val or "")
        ws.cell(row=row_idx, column=1, value=r.get("hotel_name") or "")
        ws.cell(row=row_idx, column=2, value=r.get("id1"))
        ws.cell(row=row_idx, column=3, value=id2_str)
        ws.cell(row=row_idx, column=4, value=r.get("address") or "")
        score = r.get("confidence_score")
        ws.cell(row=row_idx, column=5, value=round(score, 3) if score is not None else "")
        ws.cell(row=row_idx, column=6, value=r.get("reason") or "")

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=duplicates.xlsx"},
    )


@app.get("/api/stats")
async def api_stats() -> Dict[str, Any]:
    return get_stats()
