"""
GCNToOEmailer
=============
Decides whether to send a Target-of-Opportunity (ToO) email request and
sends it via SMTP.

Two send entry points:

* ``send_too_email()`` — synchronous, returns a success bool. Used by the
  manual Slack-button path, which needs the result to post a confirmation.
* ``send_too_email_async()`` — fire-and-forget on a shared background thread
  pool. Used by the automatic alert path so the ~1-2s TLS+AUTH handshake
  never blocks the notice handler.

Ported from ``gcn_too_emailer.py`` with the following changes:

* ``customize_too_for_neutrino()`` removed (no longer needed).
* ``evaluate_criteria()`` added — extensible list of named boolean criteria
  that determine whether an automatic ToO email should be triggered.
  Add new criteria by appending to ``GCNToOEmailer.CRITERIA``.

RASA36 body format
------------------
The RASA36 body is consumed by tcspy's mail parser
(``tcspy/utils/alertmanager/alert.py``), which recognises a fixed whitelist of
field labels and silently discards anything else. Labels in
``_build_body_rasa36`` must stay on that whitelist, and free text must pass
through ``_sanitize_for_tcspy`` first. See that function for details.

Auto-ToO master switch
----------------------
The caller (``GCNBot``) is responsible for checking
``config.turn_on_too_email_auto`` before calling ``evaluate_criteria()``.
This class is unaware of the config flag so that it remains testable in
isolation.
"""

import json
import logging
import os
import re
import smtplib
import ssl
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# tcspy mail-parser compatibility
#
# tcspy matches "<keyword><space|colon|equals><value>" ANYWHERE in a line, not
# just at the start, and the first keyword that matches wins for that line.
# A comment containing "target of opportunity" therefore overwrites the real
# target name, and "high priority please" overwrites the priority.
# ---------------------------------------------------------------------------

_TCSPY_KEYWORDS: Tuple[str, ...] = (
    'single exposure time (seconds)', 'abort current observation',
    'right ascension (r.a.)', 'single frame exposure', 'requesting user name',
    'number of telescopes', 'right ascension (ra)', 'singleframeexposure',
    'numberoftelescopes', 'declination (dec.)', 'unique identifier',
    'selectedtelnumber', 'declination (dec)', 'selectedspecfile',
    'abortobservation', 'requesting user', 'observationmode', 'selectedfilters',
    'requestinguser', 'singleexposure', 'is_observable', 'exposure time',
    'obs_starttime', 'spectral mode', 'obsstarttime', 'spectralmode',
    'is_rapid_too', 'is rapid too', 'number count', 'exposuretime',
    'is_rapidtoo', 'numbercount', 'image count', 'target name', 'ntelescopes',
    'declination', 'objecttype', 'ntelescope', 'israpidtoo', 'color mode',
    'start time', 'imagecount', 'color_mode', 'colormode', 'requestor',
    'starttime', 'requester', 'unique id', 'comments', 'specmode', 'exposure',
    'priority', 'uniqueid', 'binning', 'filters', 'exptime', 'objtype',
    'objname', 'comment', 'obsmode', 'filter', 'counts', 'target', 'object',
    'is_too', 'weight', 'count', 'notes', 'istoo', 'gain', 'uuid', 'mode',
    'r.a.', 'rank', 'dec.', 'note', 'dec', 'id', 'ra', 'de',
)

# Longest first so "image count" is neutralised before the bare "count".
_TCSPY_KEYWORDS_SORTED: List[str] = sorted(_TCSPY_KEYWORDS, key=len, reverse=True)


def _sanitize_for_tcspy(text: Any) -> str:
    """Make free text safe to embed in a tcspy-parsed mail body.

    Replaces the delimiter that follows a tcspy keyword with an underscore, so
    the phrase stays readable but no longer looks like a field assignment.
    "target of opportunity" becomes "target_of opportunity".
    """
    out = str(text) if text is not None else ''
    for keyword in _TCSPY_KEYWORDS_SORTED:
        out = re.sub(rf'(?<!\w)({re.escape(keyword)})\s*[:= ]', r'\1_', out,
                     flags=re.IGNORECASE)
    return out


def _tcspy_bool(value: Any) -> str:
    """Render a boolean as the literal tcspy accepts.

    tcspy sets the flag to 1 only when the value uppercases to exactly "TRUE"
    (alert.py:313); every other spelling, including 1, "yes" and an omitted
    line, becomes 0. So the label must always carry "True" or "False".
    """
    return "True" if str(value).strip().lower() in ("true", "yes", "1") else "False"


# ---------------------------------------------------------------------------
# Criterion functions
# Each function receives (notice_data, visibility_result) and returns bool.
# Add new criteria by appending (name, function) tuples to CRITERIA below.
# ---------------------------------------------------------------------------

def _is_observable_now(
    notice_data: Dict[str, Any],
    visibility_result: Optional[Dict[str, Any]],
) -> bool:
    """Return True when the target is currently above the horizon and visible."""
    if not visibility_result:
        return False
    return visibility_result.get("case") == "observable_now"


# ---------------------------------------------------------------------------
# Public criteria list — order matters: first match wins
# ---------------------------------------------------------------------------
_CRITERIA: List[Tuple[str, Callable]] = [
    ("observable_now", _is_observable_now),
    # Add new criteria here, e.g.:
    # ("observable_soon", _is_observable_soon),
    # ("icecube_gold",    _is_icecube_gold),
]


class GCNToOEmailer:
    """
    Send ToO email requests based on GCN notice data.

    Parameters
    ----------
    email_from : str
        Sender address (also used as the SMTP login username).
    email_to : str | list[str]
        Recipient address(es).
    email_password : str
        SMTP password / app password.
    smtp_server : str
        SMTP hostname (default: Gmail SSL).
    smtp_port : int
        SMTP port (default: 465 for SSL).
    min_altitude : float
        Minimum target altitude in degrees (informational, passed to visibility).
    min_moon_sep : float
        Minimum moon separation in degrees (informational).
    """

    # Class-level criteria list — modify to add / remove criteria globally.
    CRITERIA: List[Tuple[str, Callable]] = _CRITERIA

    # Shared thread pool for fire-and-forget (off-critical-path) sends.
    # Class-level so both telescope instances (7DT + RASA36) share workers.
    # ThreadPoolExecutor registers an atexit handler that waits for pending
    # futures, so a queued email still completes if the process is shutting down.
    _executor: Optional[ThreadPoolExecutor] = None

    # Facilities that always warrant a ToO regardless of visibility
    PRIORITY_FACILITIES = ["AMON", "IceCubeCASCADE", "HAWC", "IceCubeBRONZE", "IceCubeGOLD"]

    def __init__(
        self,
        email_from: str,
        email_to: Any,  # str or list[str]
        email_password: str,
        smtp_server: str = "smtp.gmail.com",
        smtp_port: int = 465,
        min_altitude: float = 30.0,
        min_moon_sep: float = 30.0,
        telescope: str = "7DT",
    ) -> None:
        self.email_from     = email_from
        self.email_to       = [email_to] if isinstance(email_to, str) else list(email_to)
        self.email_password = email_password
        self.smtp_server    = smtp_server
        self.smtp_port      = smtp_port
        self.min_altitude   = min_altitude
        self.min_moon_sep   = min_moon_sep
        self.telescope      = telescope
        logger.info(f"GCNToOEmailer initialised for telescope={telescope}")

    # ==================================================================
    # Public: criteria evaluation
    # ==================================================================

    # Maximum number of tiles allowed for automatic ToO triggering.
    MAX_AUTO_TOO_TILES = 5

    def evaluate_criteria(
        self,
        notice_data: Dict[str, Any],
        visibility_result: Optional[Dict[str, Any]],
        tile_result: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Run the registered criteria in order and return ``True`` on first match.

        This is the single decision point for *automatic* ToO emails.
        The caller should already have confirmed that
        ``config.turn_on_too_email_auto`` is ``True`` before calling this.

        Parameters
        ----------
        notice_data : dict
            Parsed notice data from ``GCNNoticeHandler.parse_notice()``.
        visibility_result : dict | None
            Result from ``VisibilityManager.get_status()`` — may be ``None``
            if visibility analysis failed or coordinates are unavailable.
        tile_result : dict | None
            Result from ``TileManager.get_tile_info()`` — expected to contain
            ``"n_tiles"``.  Auto-ToO is suppressed when ``n_tiles`` exceeds
            ``MAX_AUTO_TOO_TILES``.

        Returns
        -------
        bool
            ``True`` if any criterion is satisfied.

        Adding new criteria
        -------------------
        Append a ``(name, function)`` tuple to ``GCNToOEmailer.CRITERIA``::

            GCNToOEmailer.CRITERIA.append(("my_criterion", my_function))

        where ``my_function(notice_data, visibility_result) -> bool``.
        """
        # Hard gate: skip auto-ToO when the tile count exceeds the limit.
        if tile_result is not None:
            n_tiles = tile_result.get("n_tiles", 0)
            if n_tiles > self.MAX_AUTO_TOO_TILES:
                logger.info(
                    f"Auto-ToO suppressed: {n_tiles} tiles required "
                    f"(limit={self.MAX_AUTO_TOO_TILES})"
                )
                return False

        for name, fn in self.CRITERIA:
            try:
                if fn(notice_data, visibility_result):
                    logger.info(f"Auto-ToO criterion satisfied: '{name}'")
                    return True
            except Exception as exc:
                logger.warning(f"Criterion '{name}' raised an error: {exc}")
        logger.debug("No auto-ToO criteria satisfied")
        return False

    # ==================================================================
    # Public: send email
    # ==================================================================

    @classmethod
    def _get_executor(cls) -> ThreadPoolExecutor:
        """Lazily create the shared background-send thread pool."""
        if cls._executor is None:
            # 4 workers = headroom for two near-simultaneous events, each
            # dispatching a 7DT + a RASA36 send, without queueing.
            cls._executor = ThreadPoolExecutor(
                max_workers=4, thread_name_prefix="too-email"
            )
        return cls._executor

    def send_too_email_async(
        self,
        notice_data: Dict[str, Any],
        too_config: Optional[Dict[str, Any]] = None,
        on_success: Optional[Callable[[], None]] = None,
        on_failure: Optional[Callable[[], None]] = None,
    ) -> None:
        """
        Queue a ToO email on a background thread and return immediately.

        Fire-and-forget: used by the *automatic* alert path so that email
        latency (TLS handshake + SMTP AUTH, ~1-2s) never blocks the notice
        handler. Delivery success/failure is logged inside ``send_too_email``;
        callers that need the result inline must use ``send_too_email``.

        Parameters
        ----------
        on_success : callable | None
            Optional zero-arg callback invoked *on the worker thread* after a
            successful send. Used to post a Slack "ToO sent" notice.
        on_failure : callable | None
            Optional zero-arg callback invoked *on the worker thread* if the
            send fails (returns ``False`` or raises). Used to post a Slack
            failure notice.

        The emailer stays Slack-agnostic: the caller supplies the callbacks.
        Exceptions from a callback are caught and logged so a notification
        failure cannot crash the worker.
        """
        def _task() -> None:
            try:
                ok = self.send_too_email(notice_data, too_config)
            except Exception as exc:  # send_too_email already guards, belt-and-braces
                logger.error(f"Async ToO send crashed: {exc}", exc_info=True)
                ok = False
            cb = on_success if ok else on_failure
            if cb is not None:
                try:
                    cb()
                except Exception as exc:
                    which = "on_success" if ok else "on_failure"
                    logger.error(f"Async ToO {which} callback failed: {exc}", exc_info=True)

        self._get_executor().submit(_task)
        logger.debug(
            f"ToO email queued for background send (telescope={self.telescope})"
        )

    def send_too_email(
        self,
        notice_data: Dict[str, Any],
        too_config: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Compose and send a ToO request email.

        Parameters
        ----------
        notice_data : dict
        too_config : dict | None
            Observation parameter overrides.

        Returns
        -------
        bool
            ``True`` if the email was delivered successfully.
        """
        try:
            email_data = self._prepare_email_content(notice_data, too_config)
            subject    = self._build_subject(email_data)
            body       = self._build_body(email_data)

            # Write JSON attachment to a temp file
            now_str   = datetime.now().strftime("%Y%m%d%H%M%S")
            file_name = f"too_request_{now_str}.json"
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", prefix="too_request_", delete=False
            ) as fh:
                json.dump(email_data, fh, indent=4, default=str)
                file_path = fh.name

            # Build MIME message
            msg            = MIMEMultipart()
            msg["Subject"] = subject
            msg["From"]    = self.email_from
            msg["To"]      = ", ".join(self.email_to)
            msg.attach(MIMEText(body, "plain"))
            with open(file_path, "rb") as fh:
                att = MIMEApplication(fh.read(), Name=file_name)
                att["Content-Disposition"] = f'attachment; filename="{file_name}"'
                msg.attach(att)

            # Send
            ctx = ssl.create_default_context()
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port, context=ctx) as server:
                server.login(self.email_from, self.email_password)
                server.send_message(msg)

            logger.info(f"ToO email sent for {email_data.get('target')}")
            return True

        except Exception as exc:
            logger.error(f"Error sending ToO email: {exc}", exc_info=True)
            return False
        finally:
            # Clean up temp file if it was created
            if "file_path" in locals() and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except OSError:
                    pass

    # ==================================================================
    # Private helpers
    # ==================================================================

    def _prepare_email_content(
        self,
        notice_data: Dict[str, Any],
        too_config: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build the observation request dictionary."""
        if self.telescope == "RASA36":
            default: Dict[str, Any] = {
                "singleExposure":    60,
                "imageCount":        5,
                "obsmode":           "Single",
                # A GCN request is a ToO by definition; rapid is the opt-in
                # that preempts the running survey observation.
                "isToO":             "True",
                "rapidToO":          "No",
                "abortObservation":  "No",
                "priority":          "50",
                "gain":              "25",
                "binning":           "1",
            }
        else:
            default = {
                "singleExposure":    100,
                "imageCount":        3,
                "obsmode":           "Spec",
                "specmode":          "specall",
                "abortObservation":  "No",
                "priority":          "50",
                "gain":              "2750",
                "radius":            "0",
                "binning":           "1",
                "selectedFilters":   ["g", "r", "i"],
                "selectedTelNumber": 1,
            }
        cfg = {**default, **(too_config or {})}

        target_name = notice_data.get("Name", "New_GRB_Event")
        total_exp   = cfg["singleExposure"] * cfg["imageCount"]

        data: Dict[str, Any] = {
            "requester":         self.email_from,
            "target":            target_name,
            "ra":                notice_data.get("RA"),
            "dec":               notice_data.get("DEC"),
            "singleExposure":    cfg["singleExposure"],
            "imageCount":        cfg["imageCount"],
            "exposure":          total_exp,
            "obsmode":           cfg["obsmode"],
            "abortObservation":  cfg["abortObservation"],
            # Independent flags. Rapid is requested by either the explicit
            # "rapidToO" key or the older "abortObservation" one, so callers
            # written against the previous behaviour keep working.
            "isToO":             cfg.get("isToO", "True"),
            "rapidToO":          "True" if (
                _tcspy_bool(cfg.get("rapidToO", False)) == "True"
                or _tcspy_bool(cfg.get("abortObservation", False)) == "True"
            ) else "False",
            "priority":          cfg["priority"],
            "gain":              cfg["gain"],
            "binning":           cfg["binning"],
            # Kept as separate fields so the RASA36 body can emit them as a
            # machine-readable tag; tcspy uses them to tie an ingested ToO back
            # to the originating GCN notice even after a position refinement.
            "facility":          notice_data.get("Facility"),
            "trigger_num":       notice_data.get("Trigger_num"),
            "comments": (
                f"New event {target_name}. "
                f"Facility: {notice_data.get('Facility')}. "
                f"Trigger: {notice_data.get('Trigger_num')}. "
                f"Automatic ToO request from GCN Alert System."
            ),
        }

        # 7DT-only fields (RASA36 has fixed r-band, Single mode, no radius / obsStart)
        if self.telescope != "RASA36":
            data.update({
                "specmode":          cfg.get("specmode", "specall"),
                "selectedFilters":   cfg.get("selectedFilters", []),
                "selectedTelNumber": cfg.get("selectedTelNumber", 1),
                "radius":            cfg.get("radius", "0"),
                "obsStartTime":      cfg.get("obsStartTime", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            })

        if cfg.get("additional_comments"):
            data["comments"] += f" {cfg['additional_comments']}"

        return data

    def _build_subject(
        self,
        email_data: Dict[str, Any],
    ) -> str:
        return f"[Automated] {self.telescope} ToO Request for {email_data.get('target', 'Unknown')}"

    def _build_body(
        self,
        email_data: Dict[str, Any],
    ) -> str:
        if self.telescope == "RASA36":
            return self._build_body_rasa36(email_data)

        obsmode = email_data.get("obsmode", "Spec")
        if obsmode == "Spec":
            details = f"- Specmode: {email_data.get('specmode', 'specall')}"
        else:
            details = (
                f"- Filters: {','.join(email_data.get('selectedFilters', []))}\n"
                f"    - NumberOfTelescopes: {email_data.get('selectedTelNumber', 1)}"
            )

        return f"""
================================
AUTOMATIC ToO Request - GRB Alert
================================

**Observation Information**
--------------------------
- Requester: {email_data['requester']}
- Target Name: {email_data['target']}
- Right Ascension: {email_data['ra']}
- Declination:     {email_data['dec']}
- Total Exposure:  {email_data['exposure']} s
- Single Exposure: {email_data['singleExposure']} s
- # of Images:     {email_data['imageCount']}
- Obsmode:         {obsmode}
    {details}

**Detailed Settings**
--------------------
- Abort Current Observation: {email_data['abortObservation']}
- Priority:  {email_data['priority']}
- Gain:      {email_data['gain']}
- Radius:    {email_data['radius']}
- Binning:   {email_data['binning']}
- Obs Start: {email_data['obsStartTime']}
- Comments:  {email_data['comments']}

================================
THIS IS AN AUTOMATED REQUEST
PLEASE TAKE APPROPRIATE ACTION
================================
"""

    def _build_body_rasa36(self, email_data: Dict[str, Any]) -> str:
        """RASA36 body — r-band only, gain 25, Single obsmode.

        Every label here is on tcspy's parser whitelist. Renaming one makes
        tcspy fall back to its built-in default without warning, so keep the
        labels verbatim:

        * "Image Count"  -> count      (the old "# of images" matched nothing,
                                        so every request silently became 5)
        * "Total Integration" is deliberately NOT called "Total Exposure Time",
          because that label also matches tcspy's exptime key and competes
          with the single-exposure line.
        """
        # RASA36 uses two independent booleans rather than abortObservation:
        #   Is_ToO       — this is a target of opportunity at all
        #   Is_rapid_ToO — preempt the running survey observation immediately
        # Both must be emitted: tcspy reads a missing or non-"True" value as 0,
        # and DB_dynamic only treats a row as a ToO when one of them is 1.
        is_too   = _tcspy_bool(email_data.get("isToO", True))
        is_rapid = _tcspy_bool(
            email_data.get("rapidToO", email_data.get("abortObservation", False))
        )

        # tcspy discards unrecognised labels, so the GCN identifiers travel
        # inside Comments — the one free-text field that survives parsing.
        tags = [
            f"GCN_TRIGGER={email_data.get('trigger_num') or 'NA'}",
            f"GCN_FACILITY={email_data.get('facility') or 'NA'}",
            f"GCN_EVENT={_sanitize_for_tcspy(email_data.get('target'))}",
        ]
        user_comment = _sanitize_for_tcspy(email_data.get("comments", ""))
        comments = " | ".join(tags + ([user_comment] if user_comment else []))

        return f"""
====================================
AUTOMATIC ToO Request - GRB Alert
====================================

**Observation Information**
----------------------
- Requester: {email_data['requester']}
- Target Name: {email_data['target']}
- Right Ascension (R.A.): {email_data['ra']}
- Declination (Dec.): {email_data['dec']}
- Total Integration (seconds): {email_data['exposure']}
- Single Exposure Time (seconds): {email_data['singleExposure']}
- Image Count: {email_data['imageCount']}
- Obsmode: Single
- Filter: r
- Objtype: GRB
- Ntelescope: 1

**Detailed Settings**
--------------------
- Is_ToO: {is_too}
- Is_rapid_ToO: {is_rapid}
- Priority: {email_data['priority']}
- Weight: {email_data.get('weight', 1)}
- Gain: {email_data['gain']}
- Binning: {email_data['binning']}
- Comments: {comments}

====================================
THIS IS AN AUTOMATED REQUEST
PLEASE TAKE APPROPRIATE ACTION
====================================
"""