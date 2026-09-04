"""
BotConfig
=========
Centralized configuration for the GCN Alert Bot.

Loads settings from ``settings.toml`` located in the same directory as this
file (i.e. ``gcn_bot/settings.toml``).  All settings are exposed as
typed instance attributes with sensible defaults so the application can start
even when optional keys are absent from settings.toml.

Required keys (the app exits if any are missing):
    SLACK_TOKEN, SLACK_APP_TOKEN, SLACK_CHANNEL, GCN_ID, GCN_SECRET

Optional keys (fall back to defaults if absent):
    All others listed in _DEFAULTS below.
"""

import logging
import os
import sys
import tomllib
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default values for every configurable attribute
# ---------------------------------------------------------------------------
_DEFAULTS: Dict[str, Any] = {
    # Connection
    "CONNECTION_TIMEOUT": 300,

    # Slack
    "SLACK_TOKEN": "",
    "SLACK_APP_TOKEN": "",
    "SLACK_CHANNEL": "",
    "SLACK_CHANNEL_TEST": "",
    "TOO_USER_GROUP": "too-operators",

    # GCN
    "GCN_ID": "",
    "GCN_SECRET": "",

    # Visibility thresholds
    "MIN_ALTITUDE": 30,
    "MIN_MOON_SEP": 30,

    # Notice storage
    "TURN_ON_NOTICE": True,
    "OUTPUT_NOTICE_CSV": "gcn_notices.csv",
    "OUTPUT_ASCII": "grb_targets.ascii",
    "ASCII_MAX_EVENTS": 10,

    # Circular storage
    "OUTPUT_CIRCULAR_CSV": "gcn_circulars.csv",

    # Logging
    "LOG_FILE": "",  # empty = stdout only

    # Cross-matching — spatial/temporal coincidence detection
    # Overlap threshold: separation < (r1 + r2) * LENIENCY_FACTOR
    "CROSSMATCH_LENIENCY_FACTOR": 1.2,
    # Maximum trigger-time difference (hours) to consider two notices related
    "CROSSMATCH_TIME_WINDOW_HOURS": 24.0,

    # DB tile reference-image check
    "TURN_ON_DB_TILE_CHECK": False,
    # GWPortal API connection (used only when TURN_ON_DB_TILE_CHECK is true).
    # Falls back to the GWPORTAL_BASE_URL / GWPORTAL_API_KEY env vars when blank.
    "GWPORTAL_BASE_URL": "",
    "GWPORTAL_API_KEY": "",


    # ToO email — Slack-triggered
    "TURN_ON_TOO_EMAIL_SLACK": False,
    # ToO email — fully automatic (observable_now criterion)
    "TURN_ON_TOO_EMAIL_AUTO": False,

    # Email credentials
    "EMAIL_FROM": "",
    "EMAIL_TO": "",
    "EMAIL_PASSWORD": "",
    "TOO_TEST_EMAIL": "",  # Optional override for test emails (falls back to EMAIL_TO)

    # RASA36 ToO email recipient (different observatory account)
    "EMAIL_TO_RASA36": "rasa36.observation.alert@gmail.com",

    # ToO observation defaults
    "TOO_CONFIG": {
        "exptime": 100,
        "count": 3,
        "obsmode": "Spec",
        "specmode": "specall",
        "ntelescope": 1,
        "binning": "1",
        "gain": "2750",
        "priority": "50",
    },

    # GCN Kafka topics to subscribe to
    "DISPLAY_TOPICS": [
        "gcn.classic.text.AMON_NU_EM_COINC",
        "gcn.classic.text.ICECUBE_CASCADE",
        "gcn.classic.text.HAWC_BURST_MONITOR",
        "gcn.classic.text.ICECUBE_ASTROTRACK_BRONZE",
        "gcn.classic.text.ICECUBE_ASTROTRACK_GOLD",
        "gcn.classic.text.FERMI_GBM_GND_POS",
        "gcn.classic.text.FERMI_LAT_OFFLINE",
        "gcn.classic.text.SWIFT_BAT_GRB_POS_ACK",
        "gcn.classic.text.SWIFT_UVOT_POS",
        "gcn.classic.text.SWIFT_XRT_POSITION",
        "gcn.notices.einstein_probe.wxt.alert",
        "gcn.notices.svom.voevent.grm",
        "gcn.notices.svom.voevent.eclairs",
        "gcn.notices.svom.voevent.mxt",
    ],
}

# Keys that must be present and non-empty for the app to start
_REQUIRED_KEYS = ["SLACK_TOKEN", "SLACK_APP_TOKEN", "SLACK_CHANNEL", "GCN_ID", "GCN_SECRET"]


class BotConfig:
    """
    Configuration container for the GCN Alert Bot.

    Usage::

        config = BotConfig()          # loads gcn_bot/config.py automatically
        config = BotConfig("/path/to/config.py")  # explicit path

    All settings are accessible as lower-cased instance attributes, e.g.::

        config.slack_token
        config.turn_on_too_email_auto   # bool — master auto-ToO on/off
        config.display_topics           # list[str]
    """

    def __init__(self, config_path: str = "") -> None:
        # Apply defaults first.
        # Use __dict__ directly so that @property definitions (which have no
        # setter) are not triggered by setattr.
        for key, value in _DEFAULTS.items():
            self.__dict__[key.lower()] = value

        # Resolve config file path
        if not config_path:
            # Default: settings.toml in the same directory as this file
            here = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(here, "settings.toml")

        config_path = os.path.abspath(config_path)
        self._config_dir = os.path.dirname(config_path)
        self._load(config_path)
        self._validate()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_path(self, value: str) -> str:
        """Return *value* as an absolute path, anchored to the config file's directory if relative."""
        if not value or os.path.isabs(value):
            return value
        return os.path.join(self._config_dir, value)

    def _load(self, path: str) -> None:
        """Read settings.toml at *path* and copy its keys as instance attributes."""
        if not os.path.exists(path):
            logger.error(f"Settings file not found: {path}")
            logger.error("Please create settings.toml based on settings_template.toml.")
            sys.exit(1)

        try:
            with open(path, "rb") as fh:
                data = tomllib.load(fh)

            for key, value in data.items():
                self.__dict__[key.lower()] = value

            logger.info(f"Loaded {len(data)} settings from {path}")

            for default_key in _DEFAULTS:
                if default_key not in data:
                    logger.warning(
                        f"Config key '{default_key}' not found in {path} — using default value"
                    )

        except Exception as exc:
            logger.error(f"Error loading settings from {path}: {exc}")
            sys.exit(1)

    def _validate(self) -> None:
        """Exit if any required key is missing or empty."""
        missing = [k for k in _REQUIRED_KEYS if not getattr(self, k.lower(), "")]
        if missing:
            logger.error(f"Missing required config keys: {', '.join(missing)}")
            sys.exit(1)

    # ------------------------------------------------------------------
    # Convenience properties with explicit types
    # ------------------------------------------------------------------

    # All properties read from self.__dict__ directly to avoid recursion
    # (getattr would call the property getter again) and to side-step the
    # "property has no setter" guard raised by setattr.

    @property
    def connection_timeout(self) -> int:
        return int(self.__dict__.get("connection_timeout", _DEFAULTS["CONNECTION_TIMEOUT"]))

    @property
    def slack_token(self) -> str:
        return str(self.__dict__.get("slack_token", ""))

    @property
    def slack_app_token(self) -> str:
        return str(self.__dict__.get("slack_app_token", ""))

    @property
    def slack_channel(self) -> str:
        return str(self.__dict__.get("slack_channel", ""))

    @property
    def slack_channel_test(self) -> str:
        return str(self.__dict__.get("slack_channel_test", ""))

    @property
    def too_user_group(self) -> str:
        return str(self.__dict__.get("too_user_group", _DEFAULTS["TOO_USER_GROUP"]))

    @property
    def gcn_id(self) -> str:
        return str(self.__dict__.get("gcn_id", ""))

    @property
    def gcn_secret(self) -> str:
        return str(self.__dict__.get("gcn_secret", ""))

    @property
    def min_altitude(self) -> float:
        return float(self.__dict__.get("min_altitude", _DEFAULTS["MIN_ALTITUDE"]))

    @property
    def min_moon_sep(self) -> float:
        return float(self.__dict__.get("min_moon_sep", _DEFAULTS["MIN_MOON_SEP"]))

    @property
    def turn_on_notice(self) -> bool:
        return bool(self.__dict__.get("turn_on_notice", _DEFAULTS["TURN_ON_NOTICE"]))

    @property
    def output_notice_csv(self) -> str:
        # Support both OUTPUT_NOTICE_CSV (new) and OUTPUT_CSV (legacy key)
        raw = str(
            self.__dict__.get("output_notice_csv")
            or self.__dict__.get("output_csv")
            or _DEFAULTS["OUTPUT_NOTICE_CSV"]
        )
        return self._resolve_path(raw)

    @property
    def output_ascii(self) -> str:
        raw = str(self.__dict__.get("output_ascii", _DEFAULTS["OUTPUT_ASCII"]))
        return self._resolve_path(raw)

    @property
    def ascii_max_events(self) -> int:
        return int(self.__dict__.get("ascii_max_events", _DEFAULTS["ASCII_MAX_EVENTS"]))

    @property
    def turn_on_db_tile_check(self) -> bool:
        """When ``True``, query the GWPortal DB to check which required tiles
        have reference images (needs GWPORTAL_BASE_URL / GWPORTAL_API_KEY)."""
        return bool(self.__dict__.get("turn_on_db_tile_check", _DEFAULTS["TURN_ON_DB_TILE_CHECK"]))

    @property
    def gwportal_base_url(self) -> str:
        """GWPortal API base URL. Falls back to the GWPORTAL_BASE_URL env var
        when blank in settings.toml."""
        return str(self.__dict__.get("gwportal_base_url", "") or os.getenv("GWPORTAL_BASE_URL", ""))

    @property
    def gwportal_api_key(self) -> str:
        """GWPortal API key. Falls back to the GWPORTAL_API_KEY env var when
        blank in settings.toml."""
        return str(self.__dict__.get("gwportal_api_key", "") or os.getenv("GWPORTAL_API_KEY", ""))

    @property
    def crossmatch_leniency_factor(self) -> float:
        return float(self.__dict__.get("crossmatch_leniency_factor", _DEFAULTS["CROSSMATCH_LENIENCY_FACTOR"]))

    @property
    def crossmatch_time_window_hours(self) -> float:
        return float(self.__dict__.get("crossmatch_time_window_hours", _DEFAULTS["CROSSMATCH_TIME_WINDOW_HOURS"]))

    @property
    def output_circular_csv(self) -> str:
        raw = str(self.__dict__.get("output_circular_csv", _DEFAULTS["OUTPUT_CIRCULAR_CSV"]))
        return self._resolve_path(raw)

    @property
    def log_file(self) -> str:
        raw = str(self.__dict__.get("log_file", _DEFAULTS["LOG_FILE"]))
        return self._resolve_path(raw)

    @property
    def turn_on_too_email_slack(self) -> bool:
        return bool(self.__dict__.get("turn_on_too_email_slack", _DEFAULTS["TURN_ON_TOO_EMAIL_SLACK"]))

    @property
    def turn_on_too_email_auto(self) -> bool:
        """Master on/off switch for fully automatic ToO emails.

        When ``False``, no automatic ToO email is sent regardless of whether
        the observability criteria are met.  Slack-triggered ToO is unaffected
        by this flag (controlled by ``turn_on_too_email_slack``).
        """
        return bool(self.__dict__.get("turn_on_too_email_auto", _DEFAULTS["TURN_ON_TOO_EMAIL_AUTO"]))

    @property
    def email_from(self) -> str:
        return str(self.__dict__.get("email_from", ""))

    @property
    def email_to(self) -> str:
        return str(self.__dict__.get("email_to", ""))

    @property
    def email_to_rasa36(self) -> str:
        return str(self.__dict__.get("email_to_rasa36", _DEFAULTS["EMAIL_TO_RASA36"]))

    @property
    def email_password(self) -> str:
        return str(self.__dict__.get("email_password", ""))

    @property
    def too_test_email(self) -> str:
        return str(self.__dict__.get("too_test_email", ""))

    @property
    def too_config(self) -> Dict[str, Any]:
        return dict(self.__dict__.get("too_config", _DEFAULTS["TOO_CONFIG"]))

    @property
    def display_topics(self) -> List[str]:
        return list(self.__dict__.get("display_topics", _DEFAULTS["DISPLAY_TOPICS"]))

    def __repr__(self) -> str:
        return (
            f"BotConfig(channel={self.slack_channel!r}, "
            f"topics={len(self.display_topics)}, "
            f"auto_too={self.turn_on_too_email_auto})"
        )
