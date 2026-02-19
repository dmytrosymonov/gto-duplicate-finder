# GTO Hotel Duplicate Finder

A local web application that finds duplicate hotels in the GTO.UA catalog via API. Users run it on their machine and access the interface via localhost.

## Requirements

- Python 3.11+
- GTO.UA API key (request at https://gto.ua/ua/agentam)

## Quick Start

```bash
cd gto-duplicate-finder
python3 -m venv venv
source venv/bin/activate   # Linux/macOS | Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000 in a browser.

## API Key

- Enter the API key in the web form and click "Save API key"
- Or set environment variable: `export GTO_API_KEY=your_key`

## Documentation

See [docs/DEVELOPER.md](docs/DEVELOPER.md) for full developer documentation.
