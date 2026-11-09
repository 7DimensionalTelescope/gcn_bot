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

Auto-ToO enable switches
------------------------
The caller (``GCNBot``) owns the per-telescope×case enable flags
(``TURN_ON_TOO_EMAIL_AUTO_{7DT,RASA36}_{SINGLE,MULTI}``) and checks them before
acting on ``classify_auto_too()`` / ``evaluate_criteria()``. This class is
unaware of those flags so that it remains testable in isolation.
"""

import json
import logging
import os
import re
import smtplib
import socket
import ssl
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Persistent SMTP session
# ---------------------------------------------------------------------------

# Errors meaning "the connection is dead / unusable" — the send is retried once
# on a fresh connection. Anything else (e.g. every recipient refused, message
# rejected) is a real answer from the server and is raised untouched.
_STALE_CONNECTION_ERRORS = (
    smtplib.SMTPServerDisconnected,
    ConnectionError,
    socket.timeout,
    TimeoutError,
    ssl.SSLError,
)


class SMTPSession:
    """
    One logged-in SMTP connection per emailer, opened just before it is needed.

    A ToO email used to open a fresh connection per send (TCP + TLS + AUTH,
    ~1.1-1.5 s). Holding a connection open all day (NOOP keep-alive, tried
    first) was cut by Gmail after 60 min (421) and then every 5-6 min, and
    each reconnect is a login — hundreds a day, which risks Google's
    "too many login attempts" lock-out of the very account ToOs depend on.

    Instead the connection is *pre-warmed* when a send becomes likely and
    closed as soon as nobody needs it:

    * ``acquire(token, max_hold)`` takes a lease and opens the connection on a
      background thread (the caller never waits) — e.g. when a notice arrives,
      in parallel with the ~3 s visibility / tile computation that precedes the
      auto-ToO decision, or when the manual ToO form opens.
    * ``release(token)`` returns the lease; the connection is closed (QUIT) once
      no live lease remains. Closing runs in the background too and waits for
      an in-progress open/send via the lock, so the caller never blocks.
    * A lease older than its ``max_hold`` is ignored (an abandoned form, or a
      code path that never released) — no timer thread: expiry is evaluated on
      acquire / release / send.
    * ``send()`` reuses the connection (or connects lazily); on a dead
      connection it reconnects and retries once. With no live lease after the
      send, the connection is closed — without pre-warming this is exactly the
      old connect-per-send behaviour.

    A connection serialises sends, hence one session per emailer (7DT / RASA36)
    so the two telescopes still send in parallel.
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        label: str = "",
        timeout: float = 20.0,
    ) -> None:
        self.host, self.port = host, port
        self._user, self._password = user, password
        self.label = label or host
        self.timeout = timeout
        self._conn: Optional[smtplib.SMTP_SSL] = None
        self._lock = threading.Lock()          # guards _conn (open / send / close)
        self._lease_lock = threading.Lock()    # guards _leases only
        self._leases: Dict[str, float] = {}    # token -> monotonic deadline

    # -- leases -------------------------------------------------------------

    def _live_leases(self) -> int:
        """Drop expired leases; return how many are still live (lease lock held)."""
        now = time.monotonic()
        for token in [t for t, deadline in self._leases.items() if deadline <= now]:
            del self._leases[token]
            logger.info(f"SMTP[{self.label}] lease {token!r} expired without release")
        return len(self._leases)

    def acquire(self, token: str, max_hold_sec: float) -> None:
        """Take a lease and open the connection in the background (non-blocking)."""
        with self._lease_lock:
            self._leases[token] = time.monotonic() + max_hold_sec
            self._live_leases()
        threading.Thread(
            target=self._warm, name=f"smtp-warm-{self.label}", daemon=True
        ).start()

    def release(self, token: str, wait: bool = False) -> None:
        """Return a lease; close the connection when no live lease remains.

        ``wait=False`` (default) closes on a background thread so the caller
        (e.g. the Kafka loop) never waits for an in-progress open or send.
        """
        with self._lease_lock:
            self._leases.pop(token, None)
            if self._live_leases():
                return
        if wait:
            self._close_if_unused()
        else:
            threading.Thread(
                target=self._close_if_unused, name=f"smtp-close-{self.label}", daemon=True
            ).start()

    def _warm(self) -> None:
        with self._lock:
            with self._lease_lock:
                if not self._live_leases():
                    return              # released before we got here
            if self._conn is not None:
                return
            try:
                self._open()
            except Exception as exc:
                logger.warning(
                    f"SMTP[{self.label}] pre-warm failed ({type(exc).__name__}: {exc}); "
                    "the send will connect itself"
                )

    def _close_if_unused(self) -> None:
        with self._lock:
            with self._lease_lock:
                if self._live_leases():
                    return              # someone took a new lease meanwhile
            if self._conn is not None:
                self._drop()
                logger.info(f"SMTP[{self.label}] connection closed (no pending send)")

    # -- connection management (caller holds self._lock) ----------------------

    def _open(self) -> None:
        t0 = time.monotonic()
        ctx = ssl.create_default_context()
        conn = smtplib.SMTP_SSL(self.host, self.port, context=ctx, timeout=self.timeout)
        try:
            conn.login(self._user, self._password)
        except Exception:
            self._quiet_close(conn)
            raise
        self._conn = conn
        logger.info(
            f"SMTP[{self.label}] connection opened to {self.host} "
            f"({time.monotonic() - t0:.2f}s)"
        )

    @staticmethod
    def _quiet_close(conn: Optional[smtplib.SMTP]) -> None:
        if conn is None:
            return
        try:
            conn.quit()
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    def _drop(self) -> None:
        conn, self._conn = self._conn, None
        self._quiet_close(conn)

    # -- public API -----------------------------------------------------------

    def send(self, msg, from_addr: str, to_addrs: List[str]) -> Tuple[Dict[str, Any], bool]:
        """
        Send ``msg``; return ``(refused, reused)`` where ``refused`` is
        ``send_message``'s per-recipient refusal dict and ``reused`` says
        whether an already-open (pre-warmed) connection carried the send.
        """
        with self._lock:
            try:
                return self._send_locked(msg, from_addr, to_addrs)
            finally:
                with self._lease_lock:
                    unused = not self._live_leases()
                if unused and self._conn is not None:
                    self._drop()

    def _send_locked(self, msg, from_addr, to_addrs) -> Tuple[Dict[str, Any], bool]:
        reused = self._conn is not None
        for attempt in (1, 2):
            try:
                if self._conn is None:
                    self._open()
                refused = self._conn.send_message(
                    msg, from_addr=from_addr, to_addrs=to_addrs
                )
                return refused, reused
            except _STALE_CONNECTION_ERRORS as exc:
                # Connection state is unknown after a transport failure.
                self._drop()
                reused = False
                if attempt == 2:
                    raise
                logger.warning(
                    f"SMTP[{self.label}] connection unusable ({type(exc).__name__}: {exc}); "
                    "reconnecting and retrying once"
                )
            except smtplib.SMTPResponseException as exc:
                if exc.smtp_code == 421:  # "service not available, closing"
                    self._drop()
                    reused = False
                    if attempt == 2:
                        raise
                    logger.warning(f"SMTP[{self.label}] 421 from server; reconnecting and retrying once")
                    continue
                # Rejected mid-transaction: reset so the connection stays usable.
                try:
                    self._conn.rset()
                except Exception:
                    self._drop()
                raise
        raise RuntimeError("unreachable")  # pragma: no cover

    def close(self) -> None:
        """Drop all leases and close the connection (shutdown)."""
        with self._lease_lock:
            self._leases.clear()
        with self._lock:
            self._drop()


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
# 7DT observation modes and filters
#
# * Spec  — spectral mode, one of SPECMODES_7DT.
# * Color — colour mode, one of COLORMODES_7DT.
# * Deep  — any subset of FILTERS_7DT. A unit that lacks a selected filter is
#           left out and only units carrying it observe, so selecting every
#           filter means nothing gets observed. Only g, r, i are on every unit.
# ---------------------------------------------------------------------------

OBSMODES_7DT: Tuple[str, ...] = ("Spec", "Deep", "Color")
SPECMODES_7DT: Tuple[str, ...] = ("specall", "specblue", "specred")
COLORMODES_7DT: Tuple[str, ...] = ("gri", "g", "gr", "ri")
FILTERS_7DT: Tuple[str, ...] = (
    'g', 'r', 'i', 'u', 'z',
    'm375w', 'm386', 'm400', 'm425', 'm438', 'm450', 'm466w', 'm475', 'm483',
    'm500', 'm512', 'm525', 'm534', 'm550', 'm561', 'm575', 'm586', 'm600',
    'm615', 'm625', 'm640', 'm650', 'm661', 'm675', 'm692w', 'm700', 'm710w',
    'm725', 'm750', 'm769w', 'm775', 'm800', 'm825', 'm832w', 'm850', 'm875',
)
FILTERS_7DT_ALL_UNITS: Tuple[str, ...] = ("g", "r", "i")


def _canonical(value: Any, choices: Tuple[str, ...]) -> Optional[str]:
    """*value* spelled as in *choices* (case-insensitive), or None."""
    wanted = str(value or "").strip().lower()
    return next((c for c in choices if c.lower() == wanted), None)


def normalize_7dt_mode(cfg: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """Check and normalise the 7DT mode fields of *cfg*.

    Returns ``(fields, problems)``. *fields* holds ``obsmode`` plus the one
    field that mode uses (``specmode`` / ``colormode`` / ``selectedFilters``),
    spelled as 7DT expects. *problems* lists what had to be corrected; an
    invalid value falls back to the mode's default rather than being sent.
    """
    problems: List[str] = []
    obsmode = _canonical(cfg.get("obsmode"), OBSMODES_7DT)
    if obsmode is None:
        problems.append(f"unknown obsmode {cfg.get('obsmode')!r}, using Spec")
        obsmode = "Spec"

    if obsmode == "Spec":
        specmode = _canonical(cfg.get("specmode") or "specall", SPECMODES_7DT)
        if specmode is None:
            problems.append(f"unknown specmode {cfg.get('specmode')!r}, using specall")
            specmode = "specall"
        return {"obsmode": obsmode, "specmode": specmode}, problems

    if obsmode == "Color":
        colormode = _canonical(cfg.get("colormode") or "gri", COLORMODES_7DT)
        if colormode is None:
            problems.append(f"unknown colormode {cfg.get('colormode')!r}, using gri")
            colormode = "gri"
        return {"obsmode": obsmode, "colormode": colormode}, problems

    raw = cfg.get("selectedFilters") or []
    if isinstance(raw, str):
        raw = raw.split(",")
    filters: List[str] = []
    for f in raw:
        name = _canonical(f, FILTERS_7DT)
        if name is None:
            problems.append(f"unknown filter {f!r} dropped")
        elif name not in filters:
            filters.append(name)
    if not filters:
        problems.append("no filter selected, using g,r,i")
        filters = list(FILTERS_7DT_ALL_UNITS)
    return {"obsmode": obsmode, "selectedFilters": filters}, problems


AUTO_COMMENT_SOURCE = "Automatic ToO request from GCN Alert System."
MANUAL_COMMENT_SOURCE = "Manual ToO request via Slack."


def gcn_event_comment(notice_data: Dict[str, Any], source: str) -> str:
    """Comment line naming the GCN event, facility and trigger ID."""
    return (f"New event {notice_data.get('Name', 'New_GRB_Event')}. "
            f"Facility: {notice_data.get('Facility')}. "
            f"Trigger: {notice_data.get('Trigger_num')}. "
            f"{source}")


def deep_filter_note(filters: List[str]) -> str:
    """Warning for a Deep request whose filters are not on every unit ('' if none)."""
    partial = [f for f in filters if f not in FILTERS_7DT_ALL_UNITS]
    if not partial:
        return ""
    return (f"{', '.join(partial)} {'is' if len(partial) == 1 else 'are'} not on every unit: "
            "units without a selected filter are left out of the observation.")


# ---------------------------------------------------------------------------
# Criterion functions
# Each function receives (notice_data, visibility_result, emailer) and returns
# bool. The emailer instance is passed so a criterion can read per-instance
# config (e.g. observable_soon_hours). Add new criteria by appending
# (name, function) tuples to CRITERIA below.
# ---------------------------------------------------------------------------

# Facilities whose alerts are gamma-ray bursts (as opposed to the
# neutrino / particle facilities AMON, IceCube*, HAWC). Used to gate
# RASA36 auto-ToO, which fires only for GRB targets. Matches the facility
# names assigned by GCNNoticeHandler.MONITORED_FACILITIES.
_GRB_FACILITIES: frozenset = frozenset({
    "SwiftBAT", "SwiftXRT", "SwiftUVOT",
    "FermiGBM", "FermiLAT", "CALET",
    "EinsteinProbe", "SVOM",
})

# Facility-name prefixes treated as GRB sources. SVOM notices are tagged with
# the instrument ("SVOM-GRM", "SVOM-ECLAIRs", "SVOM-MXT"; see
# GCNNoticeHandler.SVOM_FACILITY_MAP), so an exact match on "SVOM" never hits.
_GRB_FACILITY_PREFIXES: tuple = ("SVOM",)


def _is_grb_facility(facility: Any) -> bool:
    """True if *facility* is a GRB source (exact name or SVOM-* instrument)."""
    name = str(facility or "").strip()
    return name in _GRB_FACILITIES or name.startswith(_GRB_FACILITY_PREFIXES)


# Fallback for how many hours ahead of the target rising an auto-ToO may still
# fire (so the request can be queued before the target is up). The live value
# is per-instance (``GCNToOEmailer.observable_soon_hours``, driven by
# ``TOO_OBSERVABLE_SOON_HOURS`` in settings.toml); this constant is only the
# default when none is supplied.
OBSERVABLE_SOON_HOURS: float = 2.0


def _is_observable_now(
    notice_data: Dict[str, Any],
    visibility_result: Optional[Dict[str, Any]],
    emailer: "GCNToOEmailer",
) -> bool:
    """Return True when the target is currently above the horizon and visible."""
    if not visibility_result:
        return False
    return visibility_result.get("case") == "observable_now"


def _is_observable_soon(
    notice_data: Dict[str, Any],
    visibility_result: Optional[Dict[str, Any]],
    emailer: "GCNToOEmailer",
) -> bool:
    """Return True when the target rises within ``emailer.observable_soon_hours``.

    Covers the ``observable_later`` case (observable later tonight) when the
    wait is short — ``observable_now`` is handled by :func:`_is_observable_now`.
    The wait time comes from the supy window
    (``details.tonight.window.time_until_start_hours``); if it is missing the
    criterion is not satisfied.
    """
    if not visibility_result:
        return False
    if visibility_result.get("case") != "observable_later":
        return False
    window = (
        (visibility_result.get("details") or {})
        .get("tonight", {})
        .get("window", {})
    )
    hrs = window.get("time_until_start_hours")
    if hrs is None:
        return False
    try:
        return 0.0 <= float(hrs) <= emailer.observable_soon_hours
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Public criteria list — order matters: first match wins
# ---------------------------------------------------------------------------
_CRITERIA: List[Tuple[str, Callable]] = [
    ("observable_now",  _is_observable_now),
    ("observable_soon", _is_observable_soon),  # rises within observable_soon_hours
    # Add new criteria here, e.g.:
    # ("icecube_gold",    _is_icecube_gold),
]

# Criteria whose match means "defer the ToO until the target is up" rather than
# send immediately. Any other satisfied criterion sends now. Used by
# classify_auto_too to return "soon" vs "now".
_DEFERRED_CRITERION_NAMES: frozenset = frozenset({"observable_soon"})


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
    smtp_prewarm : bool
        Allow :meth:`prewarm` to open the SMTP connection ahead of a likely send
        (default ``False`` → every send opens and closes its own connection).
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
        auto_too_grb_only: bool = False,
        auto_too_max_tiles: Optional[int] = None,
        observable_soon_hours: float = OBSERVABLE_SOON_HOURS,
        smtp_prewarm: bool = False,
    ) -> None:
        self.email_from     = email_from
        self.email_to       = [email_to] if isinstance(email_to, str) else list(email_to)
        self.email_password = email_password
        self.smtp_server    = smtp_server
        self.smtp_port      = smtp_port
        self.min_altitude   = min_altitude
        self.min_moon_sep   = min_moon_sep
        self.telescope      = telescope
        # Auto-ToO gating (see classify_auto_too). GRB-only restricts to GRB
        # facilities; the tile gate fires for a localization covering
        # 1..auto_too_max_tiles tiles (both telescopes use 5 — single-tile and
        # multi-tile-up-to-5 share one config per telescope).
        self.auto_too_grb_only  = auto_too_grb_only
        self.auto_too_max_tiles = (
            auto_too_max_tiles if auto_too_max_tiles is not None
            else self.MAX_AUTO_TOO_TILES
        )
        # Read by the observable_soon criterion — how many hours before the
        # target rises an auto-ToO may still fire.
        self.observable_soon_hours = observable_soon_hours
        # One SMTP connection, pre-warmed on demand and closed when unused (see
        # SMTPSession). Without pre-warming it connects per send, as before.
        self.smtp_prewarm = smtp_prewarm
        self._smtp = SMTPSession(
            smtp_server, smtp_port, email_from, email_password, label=telescope
        )
        logger.info(f"GCNToOEmailer initialised for telescope={telescope}")

    # Upper bounds on how long a pre-warm lease is honoured if never released.
    PREWARM_HOLD_AUTO_SEC   = 120.0   # notice → auto-ToO decision takes ~3 s
    PREWARM_HOLD_MANUAL_SEC = 900.0   # a person filling in the Slack form

    def prewarm(self, token: str, max_hold_sec: float = PREWARM_HOLD_AUTO_SEC) -> None:
        """Open the SMTP connection in the background ahead of a likely send.

        Non-blocking. Pair with :meth:`release_prewarm` (or pass the token as
        ``release_token`` to a send). No-op when ``smtp_prewarm`` is off.
        """
        if self.smtp_prewarm:
            self._smtp.acquire(token, max_hold_sec)

    def release_prewarm(self, token: Optional[str], wait: bool = False) -> None:
        """Give back a pre-warm lease; the connection closes once none remain."""
        if token and self.smtp_prewarm:
            self._smtp.release(token, wait=wait)

    def close_smtp(self) -> None:
        """Close the SMTP connection and forget all leases (shutdown)."""
        self._smtp.close()

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
        The caller enforces the per-telescope×case enable flags separately (see
        the module docstring); this method only evaluates the criteria/gates.

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

        where ``my_function(notice_data, visibility_result, emailer) -> bool``
        (the ``emailer`` arg gives access to per-instance config).
        """
        return self.classify_auto_too(notice_data, visibility_result, tile_result) is not None

    def classify_auto_too(
        self,
        notice_data: Dict[str, Any],
        visibility_result: Optional[Dict[str, Any]],
        tile_result: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Classify the auto-ToO decision as ``"now"``, ``"soon"``, or ``None``.

        * ``"now"``  — send immediately (target observable now).
        * ``"soon"`` — defer until the target rises (``observable_soon``).
        * ``None``   — no auto-ToO (a gate failed or no criterion matched).

        Applies the same gates as :meth:`evaluate_criteria` (GRB-only, tile
        count), then runs the criteria; a satisfied criterion in
        ``_DEFERRED_CRITERION_NAMES`` yields ``"soon"``, any other yields
        ``"now"`` (first match wins).
        """
        # GRB-only gate: RASA36 auto-ToO fires for GRBs, not neutrino/GW alerts.
        if self.auto_too_grb_only:
            facility = str(notice_data.get("Facility", "")).strip()
            if not _is_grb_facility(facility):
                logger.info(
                    f"Auto-ToO suppressed: facility '{facility}' is not a GRB source"
                )
                return None

        # Tile gate: fire for a localization covering 1..auto_too_max_tiles
        # tiles (the single-tile and multi-tile-up-to-max cases share one config
        # per telescope). A missing tile_result (no error radius → no tile
        # computation) or a zero-tile result is not a confirmed localization and
        # is suppressed.
        n_tiles = tile_result.get("n_tiles") if tile_result else None
        if n_tiles is None or not (1 <= n_tiles <= self.auto_too_max_tiles):
            logger.info(
                f"Auto-ToO suppressed: localization must cover 1-"
                f"{self.auto_too_max_tiles} tiles (got {n_tiles})"
            )
            return None

        for name, fn in self.CRITERIA:
            try:
                if fn(notice_data, visibility_result, self):
                    case = "soon" if name in _DEFERRED_CRITERION_NAMES else "now"
                    logger.info(f"Auto-ToO criterion satisfied: '{name}' → {case}")
                    return case
            except Exception as exc:
                logger.warning(f"Criterion '{name}' raised an error: {exc}")
        logger.debug("No auto-ToO criteria satisfied")
        return None

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
        release_token: Optional[str] = None,
    ) -> None:
        """
        Queue a ToO email on a background thread and return immediately.

        ``release_token``: a :meth:`prewarm` lease to give back once the send is
        done — in the same task, so the connection cannot be closed under it.

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
                ok = self.send_too_email(notice_data, too_config, release_token=release_token)
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
        release_token: Optional[str] = None,
    ) -> bool:
        """
        Compose and send a ToO request email.

        Parameters
        ----------
        notice_data : dict
        too_config : dict | None
            Observation parameter overrides.
        release_token : str | None
            :meth:`prewarm` lease to give back after the send (success or not).

        Returns
        -------
        bool
            ``True`` if the email was delivered successfully.
        """
        t_start = time.monotonic()
        try:
            email_data = self._prepare_email_content(notice_data, too_config)
            subject    = self._build_subject(email_data)
            body       = self._build_body(email_data)

            # JSON attachment, built in memory (no temp-file round trip)
            now_str   = datetime.now().strftime("%Y%m%d%H%M%S")
            file_name = f"too_request_{now_str}.json"
            payload   = json.dumps(email_data, indent=4, default=str).encode("utf-8")

            # Build MIME message
            msg            = MIMEMultipart()
            msg["Subject"] = subject
            msg["From"]    = self.email_from
            msg["To"]      = ", ".join(self.email_to)
            msg["Date"]    = formatdate(localtime=True)
            msg["Message-ID"] = make_msgid(domain=self.email_from.rsplit("@", 1)[-1])
            msg.attach(MIMEText(body, "plain"))
            att = MIMEApplication(payload, Name=file_name)
            att["Content-Disposition"] = f'attachment; filename="{file_name}"'
            msg.attach(att)
            t_built = time.monotonic()

            # Send. The envelope recipients are passed explicitly (not inferred
            # from the To header) and the per-recipient result is checked:
            # send_message() only raises when *every* recipient is refused, so a
            # partial refusal used to be logged as "sent".
            refused, reused = self._smtp.send(
                msg, from_addr=self.email_from, to_addrs=self.email_to
            )
            conn_note = "pre-warmed connection" if reused else "new connection"
            t_sent = time.monotonic()

            if refused:
                logger.error(
                    f"ToO email for {email_data.get('target')} refused by SMTP "
                    f"server for: {refused}"
                )
                return False

            logger.info(
                f"ToO email accepted by {self.smtp_server} for "
                f"{email_data.get('target')} ({self.telescope}) → "
                f"{', '.join(self.email_to)} [Message-ID {msg['Message-ID']}] "
                f"(build {t_built - t_start:.2f}s, smtp {t_sent - t_built:.2f}s, "
                f"{conn_note})"
            )
            return True

        except Exception as exc:
            logger.error(f"Error sending ToO email: {exc}", exc_info=True)
            return False
        finally:
            # Background close: the send is already done, and the caller (e.g. the
            # manual form's confirmation) should not wait ~0.2 s for QUIT.
            self.release_prewarm(release_token)

    # ==================================================================
    # Private helpers
    # ==================================================================

    def _default_config(self) -> Dict[str, Any]:
        """Telescope defaults for any observation parameter not overridden."""
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
                "colormode":         "gri",
                "abortObservation":  "No",
                "priority":          "50",
                "gain":              "2750",
                "radius":            "0",
                "binning":           "1",
                "selectedFilters":   ["g", "r", "i"],
                "selectedTelNumber": 1,
            }
        return default

    def default_form_config(
        self, too_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Effective observation config: telescope defaults overlaid by *too_config*.

        This is exactly what an email is built from, so the Slack ToO form uses
        it to pre-fill its fields — the manual form then always starts from the
        same values as the automatic request.
        """
        return {**self._default_config(), **(too_config or {})}

    def _prepare_email_content(
        self,
        notice_data: Dict[str, Any],
        too_config: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build the observation request dictionary."""
        cfg = self.default_form_config(too_config)

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
            "comments":          gcn_event_comment(notice_data, AUTO_COMMENT_SOURCE),
        }

        # 7DT-only fields (RASA36 has fixed r-band, Single mode, no radius / obsStart)
        if self.telescope != "RASA36":
            mode, problems = normalize_7dt_mode(cfg)
            for problem in problems:
                logger.warning(f"[{self.telescope}] ToO for {target_name}: {problem}")
            data.update(mode)
            data.update({
                "selectedTelNumber": cfg.get("selectedTelNumber", 1),
                "radius":            cfg.get("radius", "0"),
                # None = no scheduling constraint, observe as soon as possible.
                "obsStartTime":      cfg.get("obsStartTime") or None,
            })

        if "comments" in cfg:
            # Manual form: its comment box starts as the event line (see
            # gcn_event_comment) and is sent as edited; emptied → event line.
            data["comments"] = (str(cfg["comments"]).strip()
                                or gcn_event_comment(notice_data, MANUAL_COMMENT_SOURCE))
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

        # Layout of the 7DT ToO form mail (labels are on tcspy's whitelist).
        # Only the field of the selected obsmode follows the Obsmode line.
        obsmode = email_data.get("obsmode", "Spec")
        if obsmode == "Spec":
            mode_lines = f"\n- Specmode: {email_data.get('specmode', 'specall')}"
        elif obsmode == "Color":
            mode_lines = f"\n- Colormode: {email_data.get('colormode', 'gri')}"
        else:
            mode_lines = (
                f"\n- Filters: {','.join(email_data.get('selectedFilters', []))}"
                f"\n- Number of Telescopes: {email_data.get('selectedTelNumber', 1)}"
            )
        is_too   = _tcspy_bool(email_data.get("isToO", True))
        is_rapid = _tcspy_bool(email_data.get("rapidToO", False))

        return f"""
================================
    New ToO Request Submitted
================================

**Observation Information**
----------------------
- Requester: {email_data['requester']}
- Target Name: {email_data['target']}
- Right Ascension (R.A.): {email_data['ra']}
- Declination (Dec.): {email_data['dec']}
- Total Exposure Time (seconds): {float(email_data['exposure'])}
- Single Exposure Time (seconds): {float(email_data['singleExposure'])}
- # of images: {email_data['imageCount']}
- Obsmode: {obsmode}{mode_lines}

**Detailed Settings**
--------------------
- Is_ToO: {is_too}
- Is_rapid_ToO: {is_rapid}
- Cadence: 1
- Number of Observations: 1
- ToO start time: None
- ToO end time: None
- Priority: {email_data['priority']}
- Gain: {email_data['gain']}
- Radius (arcsec): {float(email_data.get('radius') or 0)}
- Binning: {email_data['binning']}
- Observation Start Time: {email_data.get('obsStartTime')}
- Comments: {_sanitize_for_tcspy(email_data.get('comments', ''))}
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
- Objtype: ToO
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