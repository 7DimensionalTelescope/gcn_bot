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
    "OUTPUT_TOO_SLOTS": "too_slots.json",  # per-event Auto-ToO Slack status message state
    "OUTPUT_THREAD_INDEX": "thread_index.json",  # event → Slack thread_ts (survives ASCII trimming)
    # Days an event stays in the thread index — also how long late notices and
    # follow-up circulars can still find the event's Slack thread.
    "THREAD_INDEX_MAX_AGE_DAYS": 30.0,
    # "EP Alert YYMMDD.ff" / "SVOM GRB YYMMDD.ff" circulars name the event by time
    # at 0.01-day (14.4 min) resolution, i.e. up to ±7.2 min off (observed
    # ≤ 6.8 min): match the event whose trigger (or later notice) time is within
    # this many minutes (a second, unlinked candidate → no match).
    "CIRCULAR_EP_MATCH_MINUTES": 10.0,
    "ASCII_MAX_EVENTS": 10,

    # Circular storage
    "OUTPUT_CIRCULAR_CSV": "gcn_circulars.csv",

    # Logging
    "LOG_FILE": "",  # empty = stdout only

    # Same-burst linking across facility families (gcn/linking.py has the
    # evidence for each value). A new notice is the same burst as an existing
    # event of another family when trigger times are within the window and
    # positions agree within POS_SIGMA × combined sigma (rule A), or both are
    # localized to ≤ PRECISE_DEG and agree within POS_WINDOW_HOURS (rule B).
    "SAME_EVENT_LINK": True,
    "SAME_EVENT_TIME_WINDOW_SEC": 300.0,
    "SAME_EVENT_POS_SIGMA": 3.0,
    "SAME_EVENT_SYS_DEG": {"Fermi": 5.0},      # systematic added per family (GBM errors are stat-only)
    "SAME_EVENT_ERR_FLOOR_DEG": 0.05,
    "SAME_EVENT_PRECISE_DEG": 0.5,
    "SAME_EVENT_POS_WINDOW_HOURS": 24.0,
    # A circular naming two events links them only if their triggers are this close
    "SAME_EVENT_CIRCULAR_MAX_DT_HOURS": 6.0,
    "SAME_EVENT_FAMILIES": ["Fermi", "SVOM", "Swift", "EinsteinProbe", "CALET"],

    # DB tile reference-image check
    "TURN_ON_DB_TILE_CHECK": False,
    # GWPortal API connection (used only when TURN_ON_DB_TILE_CHECK is true).
    # Falls back to the GWPORTAL_BASE_URL / GWPORTAL_API_KEY env vars when blank.
    "GWPORTAL_BASE_URL": "",
    "GWPORTAL_API_KEY": "",


    # ToO email — Slack-triggered
    "TURN_ON_TOO_EMAIL_SLACK": False,
    # ToO email — fully automatic, split by telescope AND tile case so each of
    # the four combinations is enabled independently. "single" = localization on
    # exactly 1 tile; "multi" = 2..AUTO_TOO_MAX_TILES tiles. See emailer.py.
    "TURN_ON_TOO_EMAIL_AUTO_7DT_SINGLE": False,
    "TURN_ON_TOO_EMAIL_AUTO_7DT_MULTI": False,
    "TURN_ON_TOO_EMAIL_AUTO_RASA36_SINGLE": False,
    "TURN_ON_TOO_EMAIL_AUTO_RASA36_MULTI": False,
    # Auto-ToO on a coordinate *update* (not just the first notice): lets a
    # refined position that newly qualifies — e.g. a wide Fermi GBM box shrinking
    # to a single tile once Swift/EP refines it — still trigger a ToO. Applies to
    # both telescopes' auto paths; the per-telescope switches above still gate
    # which one may send, and the ASCII duplicate-send guard prevents re-sends.
    "TURN_ON_TOO_EMAIL_AUTO_ON_UPDATE": False,
    # Auto-ToO also fires when the target rises within this many hours (not just
    # when it is observable right now), so the request can be queued ahead of the
    # target rising. Used by the observable_soon criterion.
    "TOO_OBSERVABLE_SOON_HOURS": 2.0,
    # Auto-ToO tile gate: fire only when the localization covers 1..this many
    # tiles (single-tile and multi-tile-up-to-N share one config per telescope).
    "AUTO_TOO_MAX_TILES": 5,
    # Auto-ToO restricts to GRB facilities (Swift/Fermi/CALET/EinsteinProbe/SVOM);
    # when False, any facility with a small-enough localization may trigger.
    "TOO_AUTO_GRB_ONLY": True,
    # Deferred-ToO scheduler tick — how often (seconds) pending "observable soon"
    # ToOs are checked for whether they are due to fire.
    "TOO_DEFERRED_CHECK_INTERVAL_SEC": 30.0,
    # Pre-warm the ToO SMTP connection when a send becomes likely (notice
    # arrival, manual form opened, deferred ToO about to fire) so the send
    # skips TCP+TLS+AUTH (~1.1-1.5 s); closed again as soon as it is unused.
    # False = every send opens and closes its own connection.
    "SMTP_PREWARM": True,

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

    # Automatic-ToO observation parameters, split by tile case: the single-tile
    # case (n_tiles == 1) and the multi-tile case (2..AUTO_TOO_MAX_TILES) are
    # handled separately so each can be tuned independently. Unspecified keys
    # fall through to the telescope's emailer defaults.
    # RASA36: rapid ToO, 60 s × 55 frames, top priority.
    "TOO_CONFIG_RASA36_AUTO_SINGLE": {
        "singleExposure": 60, "imageCount": 55, "priority": "1", "rapidToO": "True",
    },
    "TOO_CONFIG_RASA36_AUTO_MULTI": {
        "singleExposure": 60, "imageCount": 55, "priority": "1", "rapidToO": "True",
    },
    # 7DT: Spec mode, 100 s × 3 frames.
    "TOO_CONFIG_7DT_AUTO_SINGLE": {
        "singleExposure": 100, "imageCount": 3,
        "obsmode": "Spec", "specmode": "specall", "priority": "50",
    },
    "TOO_CONFIG_7DT_AUTO_MULTI": {
        "singleExposure": 100, "imageCount": 3,
        "obsmode": "Spec", "specmode": "specall", "priority": "50",
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
        config.turn_on_too_email_auto_7dt_single   # bool — per-telescope×case auto-ToO
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
    def output_too_slots(self) -> str:
        raw = str(self.__dict__.get("output_too_slots", _DEFAULTS["OUTPUT_TOO_SLOTS"]))
        return self._resolve_path(raw)

    @property
    def output_thread_index(self) -> str:
        raw = str(self.__dict__.get("output_thread_index", _DEFAULTS["OUTPUT_THREAD_INDEX"]))
        return self._resolve_path(raw)

    @property
    def circular_ep_match_minutes(self) -> float:
        """Time tolerance (min) for "EP Alert YYMMDD.ff" / "SVOM GRB YYMMDD.ff" circulars."""
        return max(0.0, float(self.__dict__.get(
            "circular_ep_match_minutes", _DEFAULTS["CIRCULAR_EP_MATCH_MINUTES"],
        )))

    @property
    def thread_index_max_age_days(self) -> float:
        """Retention of thread-index entries, in days (floored at 1)."""
        return max(1.0, float(self.__dict__.get(
            "thread_index_max_age_days", _DEFAULTS["THREAD_INDEX_MAX_AGE_DAYS"],
        )))

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

    def _get(self, key: str) -> Any:
        return self.__dict__.get(key.lower(), _DEFAULTS[key])

    @property
    def same_event_link(self) -> bool:
        return bool(self._get("SAME_EVENT_LINK"))

    @property
    def same_event_time_window_sec(self) -> float:
        return max(0.0, float(self._get("SAME_EVENT_TIME_WINDOW_SEC")))

    @property
    def same_event_pos_sigma(self) -> float:
        return max(0.0, float(self._get("SAME_EVENT_POS_SIGMA")))

    @property
    def same_event_sys_deg(self) -> Dict[str, float]:
        return {str(k): float(v) for k, v in dict(self._get("SAME_EVENT_SYS_DEG")).items()}

    @property
    def same_event_err_floor_deg(self) -> float:
        return max(0.0, float(self._get("SAME_EVENT_ERR_FLOOR_DEG")))

    @property
    def same_event_precise_deg(self) -> float:
        return max(0.0, float(self._get("SAME_EVENT_PRECISE_DEG")))

    @property
    def same_event_pos_window_hours(self) -> float:
        return max(0.0, float(self._get("SAME_EVENT_POS_WINDOW_HOURS")))

    @property
    def same_event_circular_max_dt_hours(self) -> float:
        return max(0.0, float(self._get("SAME_EVENT_CIRCULAR_MAX_DT_HOURS")))

    @property
    def same_event_families(self) -> List[str]:
        return [str(f) for f in self._get("SAME_EVENT_FAMILIES")]

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
    def turn_on_too_email_auto_7dt_single(self) -> bool:
        """Enable automatic 7DT ToO for single-tile (n_tiles == 1) localizations."""
        return bool(self.__dict__.get(
            "turn_on_too_email_auto_7dt_single",
            _DEFAULTS["TURN_ON_TOO_EMAIL_AUTO_7DT_SINGLE"],
        ))

    @property
    def turn_on_too_email_auto_7dt_multi(self) -> bool:
        """Enable automatic 7DT ToO for multi-tile (2..max) localizations."""
        return bool(self.__dict__.get(
            "turn_on_too_email_auto_7dt_multi",
            _DEFAULTS["TURN_ON_TOO_EMAIL_AUTO_7DT_MULTI"],
        ))

    @property
    def turn_on_too_email_auto_rasa36_single(self) -> bool:
        """Enable automatic RASA36 ToO for single-tile (n_tiles == 1) localizations."""
        return bool(self.__dict__.get(
            "turn_on_too_email_auto_rasa36_single",
            _DEFAULTS["TURN_ON_TOO_EMAIL_AUTO_RASA36_SINGLE"],
        ))

    @property
    def turn_on_too_email_auto_rasa36_multi(self) -> bool:
        """Enable automatic RASA36 ToO for multi-tile (2..max) localizations."""
        return bool(self.__dict__.get(
            "turn_on_too_email_auto_rasa36_multi",
            _DEFAULTS["TURN_ON_TOO_EMAIL_AUTO_RASA36_MULTI"],
        ))

    @property
    def turn_on_too_email_auto_on_update(self) -> bool:
        """Allow automatic ToO to fire on a coordinate *update*, not just the
        first notice for an event.

        Independent of the per-telescope master switches (which still decide
        *which* telescope may auto-send). When ``False``, auto-ToO is only
        evaluated on brand-new events.
        """
        return bool(self.__dict__.get(
            "turn_on_too_email_auto_on_update",
            _DEFAULTS["TURN_ON_TOO_EMAIL_AUTO_ON_UPDATE"],
        ))

    @property
    def too_observable_soon_hours(self) -> float:
        """Hours-ahead window for the auto-ToO ``observable_soon`` criterion."""
        return float(self.__dict__.get(
            "too_observable_soon_hours", _DEFAULTS["TOO_OBSERVABLE_SOON_HOURS"]
        ))

    @property
    def auto_too_max_tiles(self) -> int:
        """Max tile count for the auto-ToO tile gate (fires for 1..this)."""
        return int(self.__dict__.get(
            "auto_too_max_tiles", _DEFAULTS["AUTO_TOO_MAX_TILES"]
        ))

    @property
    def too_auto_grb_only(self) -> bool:
        """Restrict auto-ToO to GRB facilities (both telescopes)."""
        return bool(self.__dict__.get(
            "too_auto_grb_only", _DEFAULTS["TOO_AUTO_GRB_ONLY"]
        ))

    @property
    def too_deferred_check_interval_sec(self) -> float:
        """Deferred-ToO scheduler tick interval, in seconds."""
        return float(self.__dict__.get(
            "too_deferred_check_interval_sec",
            _DEFAULTS["TOO_DEFERRED_CHECK_INTERVAL_SEC"],
        ))

    @property
    def smtp_prewarm(self) -> bool:
        """Pre-warm the ToO SMTP connection ahead of a likely send."""
        return bool(self.__dict__.get("smtp_prewarm", _DEFAULTS["SMTP_PREWARM"]))

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
    def too_config_rasa36_auto_single(self) -> Dict[str, Any]:
        return dict(self.__dict__.get(
            "too_config_rasa36_auto_single", _DEFAULTS["TOO_CONFIG_RASA36_AUTO_SINGLE"]
        ))

    @property
    def too_config_rasa36_auto_multi(self) -> Dict[str, Any]:
        return dict(self.__dict__.get(
            "too_config_rasa36_auto_multi", _DEFAULTS["TOO_CONFIG_RASA36_AUTO_MULTI"]
        ))

    @property
    def too_config_7dt_auto_single(self) -> Dict[str, Any]:
        return dict(self.__dict__.get(
            "too_config_7dt_auto_single", _DEFAULTS["TOO_CONFIG_7DT_AUTO_SINGLE"]
        ))

    @property
    def too_config_7dt_auto_multi(self) -> Dict[str, Any]:
        return dict(self.__dict__.get(
            "too_config_7dt_auto_multi", _DEFAULTS["TOO_CONFIG_7DT_AUTO_MULTI"]
        ))

    @property
    def display_topics(self) -> List[str]:
        return list(self.__dict__.get("display_topics", _DEFAULTS["DISPLAY_TOPICS"]))

    def __repr__(self) -> str:
        auto = "".join(
            "1" if f else "0" for f in (
                self.turn_on_too_email_auto_7dt_single,
                self.turn_on_too_email_auto_7dt_multi,
                self.turn_on_too_email_auto_rasa36_single,
                self.turn_on_too_email_auto_rasa36_multi,
            )
        )
        return (
            f"BotConfig(channel={self.slack_channel!r}, "
            f"topics={len(self.display_topics)}, "
            f"auto_too[7Ds,7Dm,R36s,R36m]={auto})"
        )
