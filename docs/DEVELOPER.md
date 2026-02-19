# GTO Hotel Duplicate Finder - Developer Documentation

## Overview

This application detects duplicate hotel entries in the GTO.UA catalog. It uses the GTO.UA REST API for static data (countries, cities, hotels, hotel details) and implements a duplicate detection algorithm based on geographic proximity, name similarity, address overlap, and contact matching (phone/site).

## Architecture

```
gto-duplicate-finder/
  app/
    main.py          # FastAPI app, routes, UI
    api_client.py    # GTO API HTTP client, rate limiting, retries
    rate_limiter.py  # Token bucket rate limiter
    deduplication.py # Normalization, candidate generation, scoring
    scanner.py       # Scan orchestration (load, enrich, detect)
    config.py        # API key, base URL, env vars
  templates/
    index.html       # Main page (Jinja2)
  static/
    style.css        # Styles
    app.js           # Client-side logic
  requirements.txt
```

### Module Responsibilities

| Module | Purpose |
|--------|---------|
| `main.py` | FastAPI routes, HTML rendering, API endpoints, Excel export |
| `api_client.py` | All outbound calls to GTO API; rate limiting; retries with exponential backoff; API key passed as query param |
| `rate_limiter.py` | Token bucket limiter; shared across all API requests |
| `deduplication.py` | Name/address/site/phone normalization; candidate pairs (geo + name overlap); confidence scoring; flag rules (auto/review) |
| `scanner.py` | Loads hotels (paginated); fetches hotel_info for candidates; runs duplicate detection |
| `config.py` | Session API key storage; env var `GTO_API_KEY`; base URL |

## Tech Stack

- **Backend:** Python 3.11+, FastAPI, uvicorn
- **HTTP:** httpx (async)
- **Templates:** Jinja2
- **Frontend:** Vanilla JS, no framework
- **Export:** openpyxl (Excel)

## Installation

### Prerequisites

- Python 3.11+
- pip

### Steps

```bash
# Clone or download the project
cd gto-duplicate-finder

# Create virtual environment
python3 -m venv venv

# Activate (macOS/Linux)
source venv/bin/activate

# Activate (Windows)
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| fastapi | >=0.109.0 | Web framework |
| uvicorn[standard] | >=0.27.0 | ASGI server |
| httpx | >=0.26.0 | Async HTTP client |
| jinja2 | >=3.1.0 | HTML templating |
| openpyxl | >=3.1.0 | Excel export |

## Running the Application

### Local (single user)

```bash
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000

### Network (colleagues on same LAN)

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Share your machine IP; colleagues use http://YOUR_IP:8000

### Production (e.g. Render, Fly.io)

Use `--host 0.0.0.0` and set `PORT` from environment:

```bash
uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
```

## Configuration

### Environment Variables

| Variable | Description |
|----------|-------------|
| `GTO_API_KEY` | Default API key; optional if entered in UI |

### In-Memory State

- API key can be set via UI and stored in memory (session)
- Last scan results and progress are kept in memory
- No database; state is lost on server restart

## API Reference

### Internal API (used by the UI)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Main HTML page |
| POST | `/api/apikey` | Store API key (body: `{"apikey": "..."}`) |
| GET | `/api/countries` | List countries (query: `rps`) |
| GET | `/api/cities` | List cities (query: `country_id`, `rps`) |
| POST | `/api/scan` | Start scan (body: `{"city_ids": [...], "country_id": ?, "rps": ?}`) |
| GET | `/api/scan/status` | Poll scan progress and results |
| POST | `/api/export/excel` | Export results to Excel (body: `{"results": [...]}`) |
| GET | `/api/stats` | API request stats |

### GTO.UA API (external)

All requests go through the backend. Base URL: `https://api.gto.ua/api/v3`

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/countries` | GET | Countries (paginated) |
| `/cities` | GET | Cities by `country_id` |
| `/hotels` | GET | Hotels by `city_id`, `country_id`; pagination |
| `/hotel_info` | GET | Hotel details (site, phone) by `hotel_id` |

Authentication: `apikey` query parameter (apiKeyQueryParam per OpenAPI).

## Duplicate Detection Algorithm

### 1. Normalization

- **Name:** lowercase, remove punctuation, collapse spaces, drop stopwords (hotel, resort, etc.), normalize `&`/`and`
- **Address:** lowercase, normalize abbreviations (st., ave., etc.)
- **Site:** strip protocol, www, trailing slash
- **Phone:** digits only, optional leading `+`

### 2. Candidate Generation

- **Geo:** pairs within 250 m (or 500 m if city has &lt; 200 hotels)
- **Name overlap:** pairs sharing name tokens (length ≥ 3)
- **No coords:** pairs with overlapping name tokens if coordinates missing

### 3. Scoring

Confidence score (0–1) from:

- **DistanceScore:** 1 at &lt;50 m, 0 at &gt;2000 m
- **NameScore:** Jaccard on name tokens
- **AddressScore:** Jaccard on address tokens
- **ContactScore:** 1 if site or phone matches

Weights (when contact data exists): Contact 0.35, Distance 0.25, Name 0.25, Address 0.15. Weights are redistributed if contact data is unavailable.

### 4. Flag Rules

- **auto:** (contact match AND name ≥ 0.75) OR (distance &lt; 150 m AND name ≥ 0.88)
- **review:** confidence ≥ 0.75 without strong contact
- No flag if confidence &lt; 0.60 or distance &gt; 2 km without contact match

### 5. Clustering

Pairs are merged via union-find into clusters. Each cluster becomes one result row with all hotel names and IDs.

## Rate Limiting

- Token bucket limiter (shared across all API calls)
- Default 5 requests/second, configurable in UI
- All GTO API calls pass through the limiter

## Error Handling

- 429 / 5xx: retries with exponential backoff
- Timeouts: retries
- API key missing: 400 with clear message

## Deployment

### Render

1. Connect GitHub repo
2. Build: `pip install -r requirements.txt`
3. Start: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
4. Set `GTO_API_KEY` in environment (optional)

### Docker (example)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

## Development

### Running Tests

No automated tests are included. Manual testing via UI and API calls is recommended.

### Code Style

- Python: PEP 8
- Type hints used where practical

### Extending

- **New GTO endpoints:** add functions in `api_client.py` using `_request()`
- **Scoring changes:** edit weights and rules in `deduplication.py`
- **UI changes:** edit `templates/index.html` and `static/app.js`

## License

Apache 2.0 (per GTO API spec). Check GTO.UA terms for API usage.
