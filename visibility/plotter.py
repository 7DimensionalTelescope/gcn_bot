"""
VisibilityManager
=================
Wraps the ``supy`` visibility library and provides a uniform interface for
the rest of the bot regardless of whether ``supy`` is installed.

If ``supy`` cannot be imported ``VisibilityManager.available`` returns
``False`` and all methods return safe default values (``None`` / empty list)
so the rest of the pipeline keeps running without visibility information.

Visibility cases
----------------
``observable_now``
    Target is above the altitude threshold right now.
``observable_later``
    Target will rise above the threshold later tonight.
``observable_tomorrow``
    Target is only accessible on the next night.
``not_observable``
    Target does not reach the minimum altitude this night.

Observatories
-------------
7DT (Chile)
    Lon -70.7804, Lat -30.4704, Elev 1580 m (default supy Observer).
LOAO (Arizona, USA)
    Lon -110.7893, Lat +32.4420, Elev 2791 m (Mount Lemon).
"""

import logging
from io import BytesIO
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional import of supy
# ---------------------------------------------------------------------------
try:
    from supy.supy.observer.plotter import VisibilityPlotter as _SuPyPlotter
    from supy.supy.observer.observer import Observer as _Observer

    _supy_available = True
    logger.info("supy visibility library loaded successfully")
except ImportError as _exc:
    _supy_available = False
    _SuPyPlotter = None
    _Observer = None
    logger.warning(f"supy not available — visibility features disabled. ({_exc})")

# ---------------------------------------------------------------------------
# LOAO (Lemon Mountain Observatory / Mount Lemon, Arizona) coordinates
# ---------------------------------------------------------------------------
_LOAO_LON       = -110.7893
_LOAO_LAT       =   32.4420
_LOAO_ELEVATION =  2791.0
_LOAO_TIMEZONE  = "America/Phoenix"
_LOAO_NAME      = "LOAO"


class VisibilityManager:
    """
    Visibility analysis and plot generation for 7DT (Chile) and LOAO (Arizona).

    Parameters
    ----------
    min_altitude : float
        Minimum altitude (degrees) for a target to be considered observable.
    min_moon_sep : float
        Minimum moon separation (degrees).
    """

    def __init__(self, min_altitude: float = 30.0, min_moon_sep: float = 30.0) -> None:
        self.min_altitude = min_altitude
        self.min_moon_sep = min_moon_sep
        self._plotter:      Optional[Any] = None
        self._plotter_loao: Optional[Any] = None

        if _supy_available:
            # 7DT plotter (Chilean observatory — supy default)
            try:
                self._plotter = _SuPyPlotter()
                logger.info("VisibilityManager: 7DT supy plotter initialised")
            except Exception as exc:
                logger.warning(f"VisibilityManager: could not init 7DT supy plotter: {exc}")

            # LOAO plotter (Mount Lemon, Arizona)
            try:
                loao_observer = _Observer(
                    longitude=_LOAO_LON,
                    latitude=_LOAO_LAT,
                    elevation=_LOAO_ELEVATION,
                    timezone=_LOAO_TIMEZONE,
                    name=_LOAO_NAME,
                )
                self._plotter_loao = _SuPyPlotter(observer=loao_observer)
                logger.info("VisibilityManager: LOAO supy plotter initialised")
            except Exception as exc:
                logger.warning(f"VisibilityManager: could not init LOAO supy plotter: {exc}")

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def available(self) -> bool:
        """``True`` when the 7DT plotter is ready."""
        return self._plotter is not None

    @property
    def available_loao(self) -> bool:
        """``True`` when the LOAO plotter is ready."""
        return self._plotter_loao is not None

    # ------------------------------------------------------------------
    # Public API — 7DT (Chile)
    # ------------------------------------------------------------------

    def get_status(self, ra: float, dec: float) -> Optional[Dict[str, Any]]:
        """
        Compute 7DT visibility status for a target.

        Returns
        -------
        dict | None
            Keys: ``case``, ``message``, ``details``.
        """
        if not self.available:
            return None
        try:
            analysis = self._plotter.analyze_visibility(
                ra=ra,
                dec=dec,
                min_altitude=self.min_altitude,
                min_moon_separation=self.min_moon_sep,
            )
            return self._normalise(analysis)
        except Exception as exc:
            logger.error(f"7DT visibility analysis failed for RA={ra}, DEC={dec}: {exc}")
            return None

    def generate_plot(self, ra: float, dec: float) -> Optional[BytesIO]:
        """
        Generate a 7DT visibility plot and return it as a ``BytesIO`` PNG buffer.

        Returns ``None`` if supy is unavailable or plotting fails.
        """
        return self._generate_plot_with(self._plotter, ra, dec, "7DT")

    # ------------------------------------------------------------------
    # Public API — LOAO (Arizona)
    # ------------------------------------------------------------------

    def get_status_loao(self, ra: float, dec: float) -> Optional[Dict[str, Any]]:
        """
        Compute LOAO (Mount Lemon) visibility status for a target.

        Returns
        -------
        dict | None
            Keys: ``case``, ``message``, ``details``.
        """
        if not self.available_loao:
            return None
        try:
            analysis = self._plotter_loao.analyze_visibility(
                ra=ra,
                dec=dec,
                min_altitude=self.min_altitude,
                min_moon_separation=self.min_moon_sep,
            )
            return self._normalise(analysis)
        except Exception as exc:
            logger.error(f"LOAO visibility analysis failed for RA={ra}, DEC={dec}: {exc}")
            return None

    def generate_plot_loao(self, ra: float, dec: float) -> Optional[BytesIO]:
        """
        Generate a LOAO visibility plot and return it as a ``BytesIO`` PNG buffer.

        Returns ``None`` if supy is unavailable or plotting fails.
        """
        return self._generate_plot_with(self._plotter_loao, ra, dec, "LOAO")

    # ------------------------------------------------------------------
    # Public API — Slack blocks
    # ------------------------------------------------------------------

    def get_slack_blocks(
        self,
        visibility_result:      Optional[Dict[str, Any]],
        visibility_result_loao: Optional[Dict[str, Any]] = None,
        lc_url:     Optional[str] = None,
        notice_url: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Build Slack block elements to display visibility status and links.

        Parameters
        ----------
        visibility_result : dict | None
            7DT visibility, as returned by :meth:`get_status`.
        visibility_result_loao : dict | None
            LOAO visibility, as returned by :meth:`get_status_loao`.
        lc_url : str | None
            Light-curve URL to include as a button (optional).
        notice_url : str | None
            GCN notice URL to include as a button (optional).

        Returns
        -------
        list[dict]
            Zero or more Slack block dicts ready to append to a message.
        """
        blocks: List[Dict[str, Any]] = []

        emoji_map = {
            "observable_now":      "🟢",
            "observable_later":    "🟠",
            "observable_tomorrow": "🔵",
            "not_observable":      "🔴",
        }

        if visibility_result:
            case    = visibility_result.get("case", "")
            message = visibility_result.get("message", "")
            emoji   = emoji_map.get(case, "⚪")
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*[7DT & RASA36 (Chile)]* \n {emoji} {message}",
                    },
                }
            )

        if visibility_result_loao:
            case    = visibility_result_loao.get("case", "")
            message = visibility_result_loao.get("message", "")
            emoji   = emoji_map.get(case, "⚪")
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*[LOAO (Arizona)]* \n {emoji} {message}",
                    },
                }
            )

        # Link buttons
        button_elements: List[Dict[str, Any]] = []
        if lc_url:
            button_elements.append(
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "📈 Light Curve", "emoji": True},
                    "url": lc_url,
                    "action_id": "lc_link",
                }
            )
        if notice_url:
            button_elements.append(
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "📄 GCN Notice", "emoji": True},
                    "url": notice_url,
                    "action_id": "notice_link",
                }
            )
        if button_elements:
            blocks.append({"type": "actions", "elements": button_elements})

        return blocks

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _generate_plot_with(
        self,
        plotter: Optional[Any],
        ra: float,
        dec: float,
        label: str,
    ) -> Optional[BytesIO]:
        """Run *plotter* and return a PNG BytesIO buffer, or ``None`` on failure."""
        if plotter is None:
            return None
        try:
            import os
            analysis = plotter.analyze_visibility(
                ra=ra,
                dec=dec,
                min_altitude=self.min_altitude,
                min_moon_separation=self.min_moon_sep,
            )
            path = plotter.create_plot(analysis)
            if path is None:
                logger.warning(f"create_plot returned None for {label} RA={ra}, DEC={dec}")
                return None
            with open(path, "rb") as fh:
                buf = BytesIO(fh.read())
            os.unlink(path)
            buf.seek(0)
            return buf
        except Exception as exc:
            logger.error(f"{label} plot generation failed for RA={ra}, DEC={dec}: {exc}")
            return None

    def _normalise(self, raw: Any) -> Dict[str, Any]:
        """
        Map the supy ``analyze_visibility`` result dict to the standard format.

        supy returns:
          raw["tonight"]["status"]  → "OBSERVABLE" | "NOT_OBSERVABLE"
          raw["tonight"]["when"]    → "now" | "later" | None
          raw["next_opportunity"]   → dict with "days_from_now", or None
          raw["summary"]["formatted_message"] → human-readable string
        """
        if not isinstance(raw, dict):
            return {"case": "not_observable", "message": "Visibility analysis returned no data", "details": {}}

        tonight      = raw.get("tonight", {})
        status       = str(tonight.get("status", "")).upper()
        when         = str(tonight.get("when") or "").lower()
        next_opp     = raw.get("next_opportunity")

        if status == "OBSERVABLE":
            if when == "now":
                case    = "observable_now"
                message = tonight.get("reason") or "Target is currently observable"
                if tonight.get("window"):
                    w   = tonight["window"]
                    end = w.get("end_time_utc", "")[:16].replace("T", " ")
                    rem = w.get("time_remaining_hours", 0)
                    message = f"Observable until {end} UTC ({rem:.1f} h remaining)"
            else:
                case    = "observable_later"
                message = "Target will be observable later tonight"
                if tonight.get("window"):
                    w     = tonight["window"]
                    start = w.get("start_time_utc", "")[:16].replace("T", " ")
                    hrs   = w.get("time_until_start_hours", 0)
                    message = f"Observable from {start} UTC (in {hrs:.1f} h)"
        elif next_opp is not None:
            days = int(next_opp.get("days_from_now", 2))
            if days <= 1:
                case    = "observable_tomorrow"
                message = "Target will be observable tomorrow night"
            else:
                case    = "not_observable"
                message = f"Next opportunity in {days} days"
        else:
            case    = "not_observable"
            message = tonight.get("reason") or "Target is not observable"

        return {"case": case, "message": message, "details": raw}
