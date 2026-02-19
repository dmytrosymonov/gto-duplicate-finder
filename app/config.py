"""Configuration and session storage for API key."""
import os
from typing import Optional

# In-memory session storage (API key per session)
_session_api_key: Optional[str] = None

BASE_URL = "https://api.gto.ua/api/v3"
API_KEY_ENV = "GTO_API_KEY"
DEFAULT_RPS = 5


def get_api_key() -> Optional[str]:
    """Get API key from session or environment."""
    if _session_api_key:
        return _session_api_key
    return os.environ.get(API_KEY_ENV)


def has_saved_api_key() -> bool:
    """True if API key was explicitly saved via UI (session), not from env."""
    return bool(_session_api_key)


def set_api_key(key: str) -> None:
    """Store API key in session (in-memory only)."""
    global _session_api_key
    _session_api_key = key


def clear_api_key() -> None:
    """Clear session API key."""
    global _session_api_key
    _session_api_key = None
