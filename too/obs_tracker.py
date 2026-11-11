"""
ToO observation tracking — reads the telescopes' Slack alert channels.

The 7DT and RASA36 schedulers announce every ToO they accept in their own
Slack channel (#7dt_7ds_observation_alert, #rasa36--observation_alert):

* top-level ``:red_circle: NEW ToO Alert Request`` — the request mail was parsed
  and queued (Alert ID, tile, Note, Comments, scheduled night);
* a ``:white_check_mark: ToO Observation Success`` reply in that thread once it
  has been observed (same Alert ID);
* RASA36 only: ``Data transfer complete — `<night>_gain25_<event>_<id8>_ToO.tar```,
  in the thread or top-level.

Our requests name the event in Comments (``Facility:<F>.Trigger:<T>.`` from
``gcn_event_comment`` — whitespace is stripped by the scheduler — and
``GCN_FACILITY=…|GCN_TRIGGER=…`` on RASA36), so each announcement maps back to
``(facility, trigger)`` and hence to the event thread. Announcements without
those tags are other people's ToOs and are ignored.

:class:`ObservationTracker` reads the channels on a daemon thread
(``conversations.history`` for new top-level messages, ``conversations.replies``
for alerts still open) — only while something is pending: every
``fast_poll_sec`` while a sent request awaits its announcement (within the
receipt timeout); for an accepted request awaiting its observation, every
``observe_poll_sec`` while its target is observable now (``visibility``
callback on the announced tile centre — an observation can only complete
then), else every ``slow_poll_sec`` (waking at the window start); every
``slow_poll_sec`` for a late announcement; not at all otherwise. ``record_sent``
wakes it. An alert is done once observed (+ ``transfer_wait_hours`` for the
RASA36 data-transfer message) or after ``max_days``, and reports each state change through
``on_line(telescope, facility, trigger, text, target)``. ``target`` is the
Slack message of the request it belongs to when the send registered one (the
manual "ToO Request Submitted" confirmation) — its accumulated status lines
included — else ``None`` and main appends the line to the event's ToO slot. It also remembers every request the bot sent
(:meth:`ObservationTracker.record_sent`): one still unannounced after
``receipt_timeout_min`` gets a warning line, so a request the scheduler never
picked up (wrong recipient, scheduler down) is noticed.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from gcn.linking import family_of

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Message parsing
# ---------------------------------------------------------------------------

_ALERT_ID_RE  = re.compile(r"Alert ID:\s*([0-9a-f]{32})")
_TARGET_RE    = re.compile(r"\*Target Name:\*\s*(\S+)")
_RA_RE        = re.compile(r"\*RA:\*\s*([-+\d.]+)")
_DEC_RE       = re.compile(r"\*Dec:\*\s*([-+\d.]+)")
_NOTE_RE      = re.compile(r"\*Note:\*\s*(\S+)")
_COMMENTS_RE  = re.compile(r"\*Comments:\*\s*([^\n]*)")
_IMMEDIATE_RE = re.compile(r"\*Immediate start\?\*\s*(\w+)")
_EXPOSURE_RE  = re.compile(r"\*Exposure [Tt]ime:\*\s*([\d.]+)\s*s\s*x\s*(\d+)")
_RECEIVED_RE  = re.compile(r"\bat\s*`(\d{4}-\d\d-\d\dT[\d:.]+)`")
_SCHEDULED_RE = re.compile(r"scheduled at\(on\):[^`]*`UTC ([^`]+)`")
_COMPLETED_RE = re.compile(r"completed on:\s*`(\d{4}-\d\d-\d\dT[\d:.]+)`")
_TRANSFER_RE  = re.compile(r"_([0-9a-f]{8})_ToO\.tar`")
_TRANSFER_AT_RE = re.compile(r"\bat (\d{4}-\d\d-\d\dT[\d:.]+)")
_SENDER_RE    = re.compile(r"from the user:\s*\*([^*\s]+)")

# Event tags in Comments (the scheduler strips whitespace from Comments).
_TAG_FACILITY_RE = re.compile(r"GCN_FACILITY=([^|\s]+)")
_TAG_TRIGGER_RE  = re.compile(r"GCN_TRIGGER=([^|\s]+)")
_EVENT_LINE_RE   = re.compile(r"Facility:([A-Za-z0-9_\-]+)\.?Trigger:([A-Za-z0-9_\-]+)")


@dataclass
class AlertMessage:
    """One parsed announcement from a telescope alert channel."""

    kind: str                       # "request" | "success" | "transfer"
    alert_id: str = ""              # full id (request/success) or 8-char prefix (transfer)
    target: str = ""
    note: str = ""
    comments: str = ""
    immediate: bool = False
    exposure: Tuple[float, int] = (0.0, 0)
    at: Optional[datetime] = None   # received / completed / transferred (UTC)
    scheduled: str = ""
    sender: str = ""                # mail account the request came from
    ra: Optional[float] = None      # where the scheduler will point (tile centre)
    dec: Optional[float] = None
    extra: Dict[str, Any] = field(default_factory=dict)


def _utc(text: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def parse_alert_message(text: str) -> Optional[AlertMessage]:
    """Parse a 7DT/RASA36 alert-channel message; ``None`` if it is none of ours."""
    if not text:
        return None
    if "NEW ToO Alert Request" in text or "ToO Observation Success" in text:
        m = _ALERT_ID_RE.search(text)
        if not m:
            return None
        msg = AlertMessage(
            kind="request" if "NEW ToO Alert Request" in text else "success",
            alert_id=m.group(1),
        )
        for attr, rx in (("target", _TARGET_RE), ("note", _NOTE_RE)):
            hit = rx.search(text)
            if hit:
                setattr(msg, attr, hit.group(1))
        hit = _COMMENTS_RE.search(text)
        msg.comments = hit.group(1).strip() if hit else ""
        hit = _IMMEDIATE_RE.search(text)
        msg.immediate = bool(hit and hit.group(1).lower() in ("yes", "true"))
        hit = _EXPOSURE_RE.search(text)
        if hit:
            msg.exposure = (float(hit.group(1)), int(hit.group(2)))
        hit = (_RECEIVED_RE if msg.kind == "request" else _COMPLETED_RE).search(text)
        msg.at = _utc(hit.group(1)) if hit else None
        hit = _SCHEDULED_RE.search(text)
        msg.scheduled = hit.group(1).strip() if hit else ""
        hit = _SENDER_RE.search(text)
        msg.sender = hit.group(1).lower() if hit else ""
        ra, dec = _RA_RE.search(text), _DEC_RE.search(text)
        if ra and dec:
            msg.ra, msg.dec = float(ra.group(1)), float(dec.group(1))
        return msg
    if "Data transfer complete" in text:
        m = _TRANSFER_RE.search(text)
        if not m:
            return None
        hit = _TRANSFER_AT_RE.search(text)
        return AlertMessage(kind="transfer", alert_id=m.group(1),
                            at=_utc(hit.group(1)) if hit else None)
    return None


def event_from_comments(comments: str) -> Tuple[str, str]:
    """``(facility, trigger)`` named in a request's Comments, ``("", "")`` if none."""
    fac = _TAG_FACILITY_RE.search(comments or "")
    trig = _TAG_TRIGGER_RE.search(comments or "")
    if fac and trig and "NA" not in (fac.group(1), trig.group(1)):
        return fac.group(1), trig.group(1)
    m = _EVENT_LINE_RE.search(comments or "")
    if m and m.group(2) not in ("None", "NA"):
        return m.group(1), m.group(2)
    return "", ""


def _norm_name(name: str) -> str:
    """"GRB 261108D" and the scheduler's "GRB261108D" compare equal."""
    return re.sub(r"\s+", "", str(name or "")).lower()


def night_end(received: datetime, scheduled: str, end_hour_utc: int) -> datetime:
    """When the night the request was queued for is over (UTC).

    7DT gives the night as a bare date — the local evening's date, e.g. a
    request at 2026-11-11T01:37Z says ``2026-11-10`` — so that night ends on
    the next day. RASA36 gives a timestamp (``2026-10-16 02:04:27 (now)``),
    i.e. the night in progress. Either way: the first ``end_hour_utc`` after
    the later of the receipt time and that anchor. CTIO observing ends by
    ~10:30 UTC even in June, so 12 UTC is after any observation.
    """
    anchor = received
    s = (scheduled or "").replace("(now)", "").strip()
    try:
        if len(s) == 10:
            d = date.fromisoformat(s)
            anchor = max(anchor, datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
                         + timedelta(days=1))
        elif s:
            anchor = max(anchor, datetime.fromisoformat(s[:19]).replace(tzinfo=timezone.utc))
    except ValueError:
        pass
    end = anchor.replace(hour=end_hour_utc, minute=0, second=0, microsecond=0)
    if end <= anchor:
        end += timedelta(days=1)
    return end


# ---------------------------------------------------------------------------
# Tracker
# ---------------------------------------------------------------------------

LineCallback = Callable[[str, str, str, str, Optional[Dict[str, Any]]], None]
# (ra, dec) -> (start, end) epochs of the current / next observable window, or
# None when not observable (tonight). May raise.
WindowCallback = Callable[[float, float], Optional[Tuple[float, float]]]
_WINDOW_RECHECK_SEC = 3600.0   # re-ask "observable?" this often when there is no window
Line = Tuple[str, str, str, str, Optional[Dict[str, Any]]]


def _hm(ts: float, with_date: bool = False) -> str:
    """UTC clock time to 0.01 s ("06:02:04.18 UTC"), optionally with the date."""
    dt = datetime.fromtimestamp(ts, timezone.utc)
    fmt = "%m-%d %H:%M:%S" if with_date else "%H:%M:%S"
    return f"{dt.strftime(fmt)}.{dt.microsecond // 10000:02d} UTC"


def _dur(seconds: float) -> str:
    """Elapsed time to 0.01 s — for timing tests ("36.19 s", "18 min 05.12 s")."""
    sec = max(0.0, round(float(seconds), 2))
    if sec < 60:
        return f"{sec:.2f} s"
    m, s = divmod(sec, 60)
    m = int(m)
    if m < 60:
        return f"{m} min {s:05.2f} s"
    h, m = divmod(m, 60)
    return f"{h} h {m:02d} min {s:05.2f} s"


def _ago(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 90:
        return f"{seconds} s"
    if seconds < 5400:
        return f"{round(seconds / 60)} min"
    return f"{seconds / 3600:.1f} h"


def _event_label(name: str, facility: str, trigger: str) -> str:
    """Log label naming the event first — "SVOM-ECLAIRs #sb26110813" alone reads
    like a telescope: "GRB 261108D (SVOM-ECLAIRs #sb26110813)"."""
    ev = f"{facility} #{trigger}"
    return f"{name} ({ev})" if name else ev


def _local_hms(ts: float) -> str:
    """Host-local clock time, as in the log's own timestamps."""
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S")


class ObservationTracker:
    """Follows the bot's ToO requests through the telescopes' alert channels.

    Parameters
    ----------
    client :
        ``slack_sdk.WebClient`` (bot token; needs channels/groups:history).
    channels :
        telescope → alert channel ID; empty IDs are skipped.
    store_path :
        JSON state file (channel cursors, sent requests, open alerts).
    on_line :
        ``(telescope, facility, trigger, text, target)`` — called outside the lock.
    permalink :
        optional ``(message_ts, channel) -> url`` for the announcement link.
    visibility :
        optional :data:`WindowCallback` — sets the observation-wait cadence.
    sender :
        the bot's mail account. A request from it whose Comments lost the event
        tags (edited in the manual form) is matched by Note = event name.
    """

    def __init__(
        self,
        client: Any,
        channels: Dict[str, str],
        store_path: str,
        on_line: LineCallback,
        permalink: Optional[Callable[[str, str], str]] = None,
        fast_poll_sec: float = 10.0,
        slow_poll_sec: float = 300.0,
        observe_poll_sec: float = 30.0,
        visibility: Optional[WindowCallback] = None,
        transfer_wait_hours: float = 3.0,
        receipt_timeout_min: float = 30.0,
        night_end_hour_utc: int = 12,
        max_days: float = 3.0,
        lookback_hours: float = 24.0,
        clock: Callable[[], float] = time.time,
        sender: str = "",
    ) -> None:
        self._client = client
        self._clock = clock
        self.sender = (sender or "").lower()
        self.channels = {t: c for t, c in channels.items() if c}
        self._path = store_path
        self._on_line = on_line
        self._permalink = permalink
        self.fast_poll = max(5.0, float(fast_poll_sec))
        self.slow_poll = max(self.fast_poll, float(slow_poll_sec))
        self.observe_poll = min(self.slow_poll, max(5.0, float(observe_poll_sec)))
        self._visibility = visibility
        self.transfer_wait = float(transfer_wait_hours) * 3600.0
        self.receipt_timeout = float(receipt_timeout_min) * 60.0
        self.night_end_hour = int(night_end_hour_utc)
        self.max_age = float(max_days) * 86400.0
        self.lookback = float(lookback_hours) * 3600.0
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._mode = ""
        self._thread: Optional[threading.Thread] = None
        state = self._load()
        self._cursors: Dict[str, str] = state.get("cursors") or {}
        self._sent: List[Dict[str, Any]] = state.get("sent") or []
        self._alerts: Dict[str, Dict[str, Any]] = state.get("alerts") or {}

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> Dict[str, Any]:
        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) else {}
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as exc:
            logger.warning(f"ToO tracking state unreadable ({self._path}): {exc}")
            return {}

    def _save(self) -> None:
        tmp = f"{self._path}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"cursors": self._cursors, "sent": self._sent,
                           "alerts": self._alerts}, fh, indent=1)
            os.replace(tmp, self._path)
        except OSError as exc:
            logger.error(f"Could not persist ToO tracking state ({self._path}): {exc}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._thread or not self.channels:
            return
        self._thread = threading.Thread(target=self._run, name="too-obs-tracker", daemon=True)
        self._thread.start()
        logger.info(
            f"ToO observation tracker started ({', '.join(self.channels)}; reads every "
            f"{self.fast_poll:.0f} s awaiting acceptance, {self.slow_poll:.0f} s awaiting "
            f"observation, idle otherwise)"
        )

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    @property
    def running(self) -> bool:
        return self._thread is not None

    def tracks(self, telescope: str) -> bool:
        return telescope in self.channels

    def record_sent(self, telescope: str, facility: str, trigger: str, source: str,
                    name: str = "", message: Optional[Dict[str, Any]] = None,
                    rapid: Optional[bool] = None, requester: str = "",
                    sent_at: Optional[float] = None, ra: Optional[float] = None,
                    dec: Optional[float] = None) -> None:
        """Remember a request the bot just sent, to expect its announcement.

        ``message`` (``{channel, ts, text}``): the Slack message that shows this
        request's status — its lines are collected here and handed back with
        every ``on_line`` call for it. ``rapid``: what ``Is_rapid_ToO`` said, to
        compare with the scheduler's "Immediate start?". ``requester``: the
        mail's Requester field — the scheduler announces *that* as the user.
        ``sent_at``: when SMTP accepted the mail (epoch; default now) — the
        reference of the "after sending" times. ``ra``/``dec``: the requested
        position — which tile a pending request holds (see ``pending_requests``).
        """
        if telescope not in self.channels or not trigger:
            return
        now = self._clock()
        entry = {"telescope": telescope, "facility": str(facility), "trigger": str(trigger),
                 "name": str(name or ""), "source": source, "sent_at": sent_at or now,
                 "alert_id": "", "warned": False, "message": message, "lines": [],
                 "rapid": rapid, "requester": (requester or "").lower(),
                 "ra": ra, "dec": dec}
        with self._lock:
            # The announcement can beat the send callback by a few seconds.
            for aid, alert in self._alerts.items():
                if (not alert.get("matched") and self._same_event(entry, alert)
                        and alert["received_at"] >= now - self.receipt_timeout):
                    alert["matched"] = True
                    entry["alert_id"] = aid
                    break
            self._sent.append(entry)
            self._save()
        self._wake.set()  # start reading now

    def pending_requests(self, telescope: str, facility: str, trigger: str) -> List[Dict[str, Any]]:
        """Requests of this event to *telescope* that are still open.

        Open = sent and awaiting acceptance (within the receipt timeout), or
        accepted and not yet observed (no Success / ToO data transfer). Released:
        observed, not announced within the timeout (lost — may be re-sent), or
        no longer followed (``max_days``). Each item is the sent entry plus
        ``accepted_at`` / ``tile`` (the scheduler's tile name) when accepted.
        """
        probe = {"telescope": telescope, "facility": str(facility), "trigger": str(trigger)}
        now = self._clock()
        out: List[Dict[str, Any]] = []
        with self._lock:
            for entry in self._sent:
                if not self._same_event(entry, probe):
                    continue
                aid = entry.get("alert_id")
                if not aid:
                    if not entry.get("warned") and now - entry["sent_at"] <= self.receipt_timeout:
                        out.append({**entry, "accepted_at": None, "tile": ""})
                    continue
                alert = self._alerts.get(aid)
                if alert and not (alert.get("observed_at") or alert.get("transferred_at")):
                    out.append({**entry, "accepted_at": alert["received_at"],
                                "tile": alert.get("target", "")})
        return out

    def poll_once(self, everything: bool = False) -> Optional[float]:
        """One pass: new announcements, replies of open alerts, timeouts.

        Reads only the channels of telescopes with something pending
        (``everything``: all of them — the catch-up pass at start-up). Returns
        the delay until the next pass, ``None`` when nothing is pending.
        """
        lines: List[Line] = []
        now = self._clock()
        with self._lock:
            _, active = self._pending(now)
        for telescope, channel in self.channels.items():
            if everything or telescope in active:
                for msg in self._fetch_history(channel):
                    with self._lock:
                        self._handle(telescope, channel, msg, lines)
        # observations: each open alert on its own schedule (see _alert_interval)
        with self._lock:
            due = [(aid, dict(a)) for aid, a in self._alerts.items()
                   if self._is_open(a) and (everything or now >= a.get("next_check", 0))]
        for aid, alert in due:
            for msg in self._fetch_replies(alert["channel"], alert["ts"]):
                with self._lock:
                    self._handle(alert["telescope"], alert["channel"], msg, lines)
        for aid, alert in due:
            with self._lock:
                current = dict(self._alerts.get(aid) or alert)   # may be observed now
            interval = self._alert_interval(current, self._clock())  # outside the lock
            with self._lock:
                if aid in self._alerts:
                    self._alerts[aid]["vis"] = current.get("vis")
                    self._alerts[aid]["in_window"] = current.get("in_window", False)
                    self._alerts[aid]["next_check"] = self._clock() + interval
        with self._lock:
            self._check_timeouts(self._clock(), lines)
            self._prune(self._clock())
            self._save()
            fast, active = self._pending(self._clock())
            now = self._clock()
            open_alerts = [a for a in self._alerts.values() if self._is_open(a)]
            waits = [max(1.0, a.get("next_check", 0) - now) for a in open_alerts]
            watching = sorted({a["telescope"] for a in open_alerts if a.get("in_window")})
        for telescope, facility, trigger, text, target in lines:
            try:
                self._on_line(telescope, facility, trigger, text, target)
            except Exception as exc:
                logger.error(f"ToO tracking line failed ({telescope} {facility} #{trigger}): {exc}",
                             exc_info=True)
        mode = ("fast" if fast else f"observable now ({', '.join(watching)})" if watching
                else "slow" if active else "idle")
        if mode != self._mode:
            logger.info(f"ToO tracker: {mode}"
                        + (f" — pending {', '.join(sorted(active))}" if active else ""))
            self._mode = mode
        if not active:
            return None
        delays = waits + [self.slow_poll] + ([self.fast_poll] if fast else [])
        return min(delays)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _run(self) -> None:
        first = True
        while not self._stop.is_set():
            try:
                delay = self.poll_once(everything=first)
            except Exception as exc:
                logger.error(f"ToO observation tracker poll failed: {exc}", exc_info=True)
                delay = self.slow_poll
            first = False
            self._wake.wait(delay)   # None: sleep until record_sent / stop
            self._wake.clear()

    def _pending(self, now: float) -> Tuple[set, set]:
        """Telescopes awaiting an announcement within the timeout (read fast) and
        all telescopes with anything still open (late announcement, observation)."""
        unannounced = [e for e in self._sent if not e.get("alert_id")]
        fast = {e["telescope"] for e in unannounced if now - e["sent_at"] <= self.receipt_timeout}
        active = fast | {e["telescope"] for e in unannounced}
        active |= {a["telescope"] for a in self._alerts.values() if self._is_open(a)}
        return fast, active

    def _fetch_history(self, channel: str) -> List[Dict[str, Any]]:
        """Top-level messages newer than the channel cursor, oldest first."""
        # never further back than an alert is followed (an idle channel's
        # cursor can be old)
        floor = self._clock() - min(self.lookback, self.max_age) if channel not in self._cursors \
            else self._clock() - self.max_age
        oldest = f"{max(float(self._cursors.get(channel) or 0), floor):.6f}"
        out: List[Dict[str, Any]] = []
        cursor = None
        try:
            while True:
                kwargs: Dict[str, Any] = {"channel": channel, "oldest": oldest, "limit": 200}
                if cursor:
                    kwargs["cursor"] = cursor
                resp = self._client.conversations_history(**kwargs)
                out.extend(resp.get("messages") or [])
                cursor = (resp.get("response_metadata") or {}).get("next_cursor")
                if not (resp.get("has_more") and cursor):
                    break
        except Exception as exc:
            logger.warning(f"ToO tracker: history of {channel} failed: {exc}")
            return []
        out.sort(key=lambda m: float(m.get("ts", 0)))
        with self._lock:
            self._cursors[channel] = out[-1]["ts"] if out else oldest
        return out

    def _fetch_replies(self, channel: str, ts: str) -> List[Dict[str, Any]]:
        try:
            resp = self._client.conversations_replies(channel=channel, ts=ts, limit=200)
        except Exception as exc:
            logger.warning(f"ToO tracker: replies of {channel}/{ts} failed: {exc}")
            return []
        return [m for m in (resp.get("messages") or []) if m.get("ts") != ts]

    @staticmethod
    def _same_event(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
        return (a["telescope"] == b["telescope"] and str(a["trigger"]) == str(b["trigger"])
                and family_of(a["facility"]) == family_of(b["facility"]))

    def _alert_interval(self, alert: Dict[str, Any], now: float) -> float:
        """Seconds until an open alert's replies are read again.

        Observable now → ``observe_poll``; before tonight's window → ``slow_poll``
        but waking at its start; otherwise (not observable, unknown, already
        observed and only awaiting the RASA36 transfer) → ``slow_poll``. Our
        altitude / Moon limits are not the scheduler's, so outside the window
        it still looks, just slowly.
        """
        alert["in_window"] = False
        if alert.get("observed_at"):
            return self.slow_poll
        window = self._window(alert, now)
        if window is None:
            return self.slow_poll
        start, end = window
        if start <= now < end:
            alert["in_window"] = True
            return min(self.observe_poll, max(1.0, end - now))
        if now < start:
            return max(1.0, min(self.slow_poll, start - now))
        return self.slow_poll

    def _window(self, alert: Dict[str, Any], now: float) -> Optional[Tuple[float, float]]:
        """Cached observable window of the alert's target ([start, end, checked])."""
        if self._visibility is None or alert.get("ra") is None:
            return None
        vis = alert.get("vis")
        stale = (vis is None
                 or (vis[1] is not None and now >= vis[1])                 # window over
                 or (vis[1] is None and now - vis[2] >= _WINDOW_RECHECK_SEC))
        if stale:
            try:
                win = self._visibility(alert["ra"], alert["dec"])
            except Exception as exc:
                logger.warning(f"ToO tracker: visibility of {alert['ra']}, {alert['dec']} "
                               f"failed: {exc}")
                win = None
            vis = [win[0], win[1], now] if win else [None, None, now]
            alert["vis"] = vis
        return (vis[0], vis[1]) if vis[0] is not None else None

    def _is_open(self, alert: Dict[str, Any]) -> bool:
        now = self._clock()
        if now - alert["received_at"] > self.max_age:
            return False
        if not alert.get("observed_at"):
            return True
        # observed: keep watching a little for the RASA36 data-transfer message
        return not alert.get("transferred_at") and now - alert["observed_at"] < self.transfer_wait

    def _handle(self, telescope: str, channel: str, raw: Dict[str, Any],
                lines: List[Line]) -> None:
        msg = parse_alert_message(raw.get("text") or "")
        if msg is None:
            return
        if msg.kind == "request":
            if msg.alert_id in self._alerts:
                return
            received = msg.at or datetime.fromtimestamp(float(raw["ts"]), timezone.utc)
            facility, trigger = event_from_comments(msg.comments)
            if not trigger:
                sent = self._sent_by_note(telescope, msg, received.timestamp())
                if sent is None:
                    return  # not a request of ours
                facility, trigger = sent["facility"], sent["trigger"]
            end = night_end(received, msg.scheduled, self.night_end_hour)
            alert = {
                "telescope": telescope, "channel": channel, "ts": raw["ts"],
                "facility": facility, "trigger": trigger, "target": msg.target,
                "note": msg.note, "received_at": received.timestamp(),
                "ra": msg.ra, "dec": msg.dec,
                "immediate": msg.immediate, "night_end": end.timestamp(),
                "night": (end - timedelta(days=1)).strftime("%m-%d"),
                "matched": False, "observed_at": None, "transferred_at": None,
                "night_noted": False,
            }
            self._alerts[msg.alert_id] = alert
            latency = self._match_sent(msg.alert_id, alert)
            entry = self._entry_for(msg.alert_id)
            label = _event_label((entry or {}).get("name") or msg.note, facility, trigger)
            sent = f", sent {_local_hms(entry['sent_at'])}" if entry else ""
            logger.info(f"ToO tracker: {telescope} accepted the {label} request{sent} "
                        f"(alert {msg.alert_id[:8]}, {msg.target})")
            self._emit(lines, telescope, facility, trigger,
                       self._received_line(alert, latency, entry), entry)
        elif msg.kind == "success":
            alert = self._alerts.get(msg.alert_id)
            if not alert or alert.get("observed_at"):
                return
            done = (msg.at or datetime.fromtimestamp(float(raw["ts"]), timezone.utc)).timestamp()
            alert["observed_at"] = done
            exp, n = msg.exposure
            exp_txt = f" ({exp:g} s × {n})" if n else ""
            entry = self._entry_for(msg.alert_id)
            after = f", {_dur(done - entry['sent_at'])} after sending" if entry else ""
            label = _event_label((entry or {}).get("name") or alert.get("note", ""),
                                 alert["facility"], alert["trigger"])
            logger.info(f"ToO tracker: {telescope} observed the {label} request "
                        f"(alert {msg.alert_id[:8]}{after})")
            self._emit(lines, telescope, alert["facility"], alert["trigger"],
                       f":camera_with_flash: observed — completed {_hm(done, True)}{after}"
                       f"{exp_txt}", entry)
        elif msg.kind == "transfer":
            for aid, alert in self._alerts.items():
                if alert["telescope"] == telescope and aid.startswith(msg.alert_id):
                    if alert.get("transferred_at"):
                        return
                    when = (msg.at or datetime.fromtimestamp(float(raw["ts"]),
                                                             timezone.utc)).timestamp()
                    alert["transferred_at"] = when
                    self._emit(lines, telescope, alert["facility"], alert["trigger"],
                               f":package: data transferred {_hm(when, True)}",
                               self._entry_for(aid))
                    return

    def _entry_for(self, alert_id: str) -> Optional[Dict[str, Any]]:
        return next((e for e in self._sent if e.get("alert_id") == alert_id), None)

    @staticmethod
    def _emit(lines: List[Line], telescope: str, facility: str, trigger: str, text: str,
              entry: Optional[Dict[str, Any]] = None) -> None:
        """Queue a line; a request with its own status message collects it there."""
        target = None
        if entry and entry.get("message"):
            entry.setdefault("lines", []).append(text)
            target = {**entry["message"], "lines": list(entry["lines"])}
        lines.append((telescope, facility, trigger, text, target))

    def _sent_by_note(self, telescope: str, msg: AlertMessage,
                      received: float) -> Optional[Dict[str, Any]]:
        """Our unmatched send this untagged request answers: same telescope, from
        the bot's account or that send's Requester (the scheduler announces the
        mail's Requester field as the user), Note = event name, within the
        receipt timeout."""
        if not msg.note:
            return None
        for entry in self._sent:
            if (not entry.get("alert_id") and entry["telescope"] == telescope
                    and msg.sender and msg.sender in (self.sender, entry.get("requester"))
                    and _norm_name(entry.get("name")) == _norm_name(msg.note)
                    and -120 <= received - entry["sent_at"] <= self.receipt_timeout):
                return entry
        return None

    def _match_sent(self, alert_id: str, alert: Dict[str, Any]) -> Optional[float]:
        """Pair the announcement with the oldest unmatched send; its latency (s)."""
        for entry in self._sent:
            if (not entry.get("alert_id") and self._same_event(entry, alert)
                    and entry["sent_at"] <= alert["received_at"] + 120):
                entry["alert_id"] = alert_id
                alert["matched"] = True
                return alert["received_at"] - entry["sent_at"]
        return None

    def _received_line(self, alert: Dict[str, Any], latency: Optional[float],
                       entry: Optional[Dict[str, Any]] = None) -> str:
        when = "immediate start" if alert["immediate"] else f"queued for the night of {alert['night']}"
        lat = f", {_dur(latency)} after sending" if latency is not None else ""
        link = ""
        if self._permalink:
            try:
                url = self._permalink(alert["ts"], alert["channel"])
            except Exception:
                url = ""
            link = f" · <{url}|announcement>" if url else ""
        # the scheduler's "Immediate start?" is how it read Is_rapid_ToO
        rapid = (entry or {}).get("rapid")
        mismatch = ""
        if rapid is not None and bool(rapid) != bool(alert["immediate"]):
            mismatch = (f" · :warning: sent as {'rapid' if rapid else 'normal'}, announced as "
                        f"{'immediate start' if alert['immediate'] else 'not immediate'}")
        return (f":inbox_tray: accepted {_hm(alert['received_at'])}{lat} — "
                f"tile {alert['target'] or '?'}, {when}{link}{mismatch}")

    def _check_timeouts(self, now: float, lines: List[Line]) -> None:
        for entry in self._sent:
            if (not entry.get("alert_id") and not entry.get("warned")
                    and now - entry["sent_at"] > self.receipt_timeout):
                entry["warned"] = True
                channel = self.channels.get(entry["telescope"], "")
                label = _event_label(entry.get("name", ""), entry["facility"], entry["trigger"])
                logger.warning(f"ToO tracker: no {entry['telescope']} announcement for the "
                               f"{label} request sent {_local_hms(entry['sent_at'])} "
                               f"({_ago(now - entry['sent_at'])} ago)")
                self._emit(lines, entry["telescope"], entry["facility"], entry["trigger"],
                           f":warning: not announced in <#{channel}> "
                           f"{_ago(now - entry['sent_at'])} after sending — the request "
                           f"may not have been accepted", entry)
        for aid, alert in self._alerts.items():
            # a ToO data transfer also proves the observation (RASA36 sometimes
            # posts no Success message — EP 261010a)
            if (not alert.get("observed_at") and not alert.get("transferred_at")
                    and not alert.get("night_noted") and now > alert["night_end"]
                    and now - alert["received_at"] <= self.max_age):  # replies still read
                alert["night_noted"] = True
                self._emit(lines, alert["telescope"], alert["facility"], alert["trigger"],
                           f":hourglass: not observed on the night of {alert['night']} "
                           f"(may still be queued)", self._entry_for(aid))

    def _prune(self, now: float) -> None:
        self._sent = [e for e in self._sent if now - e["sent_at"] <= self.max_age]
        self._alerts = {k: a for k, a in self._alerts.items()
                        if now - a["received_at"] <= self.max_age}
