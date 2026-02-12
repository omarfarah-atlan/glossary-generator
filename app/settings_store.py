"""Persistent settings storage with file-based fallback and caching."""

import json
import logging
import os
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta

from app.models import AppSettings

logger = logging.getLogger(__name__)

# In-memory cache to reduce file/Dapr access
_settings_cache: Optional[AppSettings] = None
_cache_timestamp: Optional[datetime] = None
_CACHE_TTL = timedelta(seconds=30)  # Cache for 30 seconds
_dapr_available = None  # None = unknown, True = available, False = unavailable
_dapr_check_timestamp: Optional[datetime] = None
_DAPR_RETRY_INTERVAL = timedelta(minutes=5)  # Retry Dapr every 5 minutes

# Settings file path - stored in local directory for persistence
SETTINGS_FILE = Path(__file__).parent.parent / "local" / "settings.json"
DAPR_STORE_NAME = "statestore"
DAPR_SETTINGS_KEY = "app_settings"


def _ensure_local_dir():
    """Ensure the local directory exists."""
    SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)


def load_settings_from_file() -> Optional[AppSettings]:
    """Load settings from local JSON file."""
    try:
        if SETTINGS_FILE.exists():
            with open(SETTINGS_FILE, "r") as f:
                data = json.load(f)
                return AppSettings(**data)
    except Exception as e:
        logger.warning(f"Could not load settings from file: {e}")
    return None


def save_settings_to_file(settings: AppSettings) -> bool:
    """Save settings to local JSON file."""
    try:
        _ensure_local_dir()
        with open(SETTINGS_FILE, "w") as f:
            json.dump(settings.model_dump(), f, indent=2)
        logger.info(f"Settings saved to {SETTINGS_FILE}")
        return True
    except Exception as e:
        logger.error(f"Could not save settings to file: {e}")
        return False


def _should_skip_dapr() -> bool:
    """Check if we should skip Dapr (known to be unavailable)."""
    global _dapr_available, _dapr_check_timestamp

    if _dapr_available is True:
        return False

    if _dapr_available is False:
        # Check if retry interval has passed
        if _dapr_check_timestamp and datetime.now() - _dapr_check_timestamp < _DAPR_RETRY_INTERVAL:
            return True  # Skip, still in cooldown

    return False  # Unknown or retry interval passed, try Dapr


def _mark_dapr_status(available: bool):
    """Mark Dapr availability status."""
    global _dapr_available, _dapr_check_timestamp
    _dapr_available = available
    _dapr_check_timestamp = datetime.now()


def load_settings_from_dapr() -> Optional[AppSettings]:
    """Load settings from Dapr state store."""
    # Fast-fail if Dapr is known to be unavailable
    if _should_skip_dapr():
        return None

    try:
        from dapr.clients import DaprClient
        # Set a short timeout for the health check
        with DaprClient(timeout=5) as client:
            state = client.get_state(store_name=DAPR_STORE_NAME, key=DAPR_SETTINGS_KEY)
            if state.data:
                _mark_dapr_status(True)
                return AppSettings(**json.loads(state.data))
            _mark_dapr_status(True)  # Connected but no data
    except Exception as e:
        _mark_dapr_status(False)
        logger.debug(f"Dapr unavailable (using file storage): {type(e).__name__}")
    return None


def save_settings_to_dapr(settings: AppSettings) -> bool:
    """Save settings to Dapr state store."""
    # Fast-fail if Dapr is known to be unavailable
    if _should_skip_dapr():
        return False

    try:
        from dapr.clients import DaprClient
        with DaprClient(timeout=5) as client:
            client.save_state(
                store_name=DAPR_STORE_NAME,
                key=DAPR_SETTINGS_KEY,
                value=json.dumps(settings.model_dump()),
            )
        _mark_dapr_status(True)
        return True
    except Exception:
        _mark_dapr_status(False)
        return False


def _is_cache_valid() -> bool:
    """Check if the settings cache is still valid."""
    if _settings_cache is None or _cache_timestamp is None:
        return False
    return datetime.now() - _cache_timestamp < _CACHE_TTL


def load_settings(force_refresh: bool = False) -> AppSettings:
    """Load settings with caching and fallback chain: cache -> file -> Dapr -> defaults.

    Priority:
    1. In-memory cache (fastest)
    2. Local file (most reliable for persistence)
    3. Dapr state store (runtime state)
    4. Environment variables (for initial setup)
    5. Default values
    """
    global _settings_cache, _cache_timestamp

    # Return cached settings if valid and not forcing refresh
    if not force_refresh and _is_cache_valid():
        return _settings_cache

    # Try file first (primary persistent storage)
    settings = load_settings_from_file()
    if settings:
        # Sync to Dapr for runtime access (non-blocking)
        save_settings_to_dapr(settings)
        _settings_cache = settings
        _cache_timestamp = datetime.now()
        return settings

    # Try Dapr (might have settings from current session)
    settings = load_settings_from_dapr()
    if settings:
        # Persist to file for next restart
        save_settings_to_file(settings)
        _settings_cache = settings
        _cache_timestamp = datetime.now()
        return settings

    # Build from environment variables
    settings = AppSettings(
        anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY"),
        atlan_api_key=os.environ.get("ATLAN_API_KEY"),
        atlan_base_url=os.environ.get("ATLAN_BASE_URL"),
        llm_proxy_url=os.environ.get("LLM_PROXY_URL", "https://llmproxy.atlan.dev"),
        claude_model=os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514"),
        default_glossary_qn=os.environ.get("DEFAULT_GLOSSARY_QN"),
        snowflake_account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        snowflake_user=os.environ.get("SNOWFLAKE_USER"),
        snowflake_warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        snowflake_database=os.environ.get("SNOWFLAKE_DATABASE", "MDLH_GOLD_RKO"),
        snowflake_schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
        snowflake_role=os.environ.get("SNOWFLAKE_ROLE"),
    )

    # If we got any real values from env, persist them
    if settings.anthropic_api_key or settings.atlan_base_url or settings.snowflake_account:
        save_settings(settings)

    _settings_cache = settings
    _cache_timestamp = datetime.now()
    return settings


def invalidate_cache():
    """Invalidate the settings cache (call after saving)."""
    global _settings_cache, _cache_timestamp
    _settings_cache = None
    _cache_timestamp = None


def save_settings(settings: AppSettings) -> bool:
    """Save settings to both file and Dapr for persistence and runtime access."""
    global _settings_cache, _cache_timestamp

    file_saved = save_settings_to_file(settings)
    dapr_saved = save_settings_to_dapr(settings)

    # Update cache immediately
    if file_saved or dapr_saved:
        _settings_cache = settings
        _cache_timestamp = datetime.now()
        logger.info("Settings saved and cached")

    return file_saved or dapr_saved


def get_settings_dict() -> dict:
    """Get settings as a dictionary (for backward compatibility)."""
    return load_settings().model_dump()
