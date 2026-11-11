"""
GCNBot
======
Top-level orchestrator that wires together every component and runs the
main Kafka consumption loop.

Usage (production)::

    python main.py

Usage (development — process _TEST topics and print to Slack)::

    python main.py --include-test --send

Command-line flags
------------------
--include-test
    Process GCN topics whose name contains ``_TEST``.  By default such
    topics are silently skipped.
--send
    Actually post messages to Slack (useful when ``--include-test`` is
    active and you still want real notifications).
--config PATH
    Path to an alternative ``settings.toml`` file.
"""

import argparse
import logging
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, List, Optional

from gcn import linking
from slack.too_slot import ToOSlot
from too.emailer import too_is_rapid
from too.obs_tracker import ObservationTracker
from too.scheduler import DeferredEntry, DeferredToOScheduler

# ---------------------------------------------------------------------------
# Logging — configure before importing sub-modules so their loggers inherit
# ---------------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s [%(levelname)-7s] %(name)-20s — %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    datefmt=LOG_DATEFMT,
)

# Quieten noisy third-party loggers so the startup log shows only the bot's
# own messages. numexpr in particular emits several INFO lines about core
# counts the moment it is imported (transitively, via numpy/astropy).
for _noisy in ("slack_bolt", "numexpr", "numexpr.utils"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def _log_banner(title: str, lines: "Optional[list]" = None) -> None:
    """Emit a visually distinct section header (and optional body lines).

    Groups startup output into readable blocks instead of a flat stream.
    """
    bar = "─" * 55
    logger.info(bar)
    logger.info(title)
    if lines:
        for line in lines:
            logger.info(f"  • {line}")
    logger.info(bar)


_KST = timezone(timedelta(hours=9))
_MAX_TOO_SLOTS = 200  # in-memory ToOSlot cache bound (oldest dropped first)


class GCNBot:
    """
    Orchestrates all GCN-bot components.

    Parameters
    ----------
    config : BotConfig
        Loaded configuration object.
    send_to_slack : bool
        When ``False`` messages are processed but never posted.
        Defaults to ``True`` (production behaviour).
    include_test_topics : bool
        When ``True`` topics whose name contains ``_TEST`` are processed.
        Defaults to ``False``.
    """

    # Topics that carry GCN Circulars (routed to circular_handler)
    CIRCULAR_TOPIC_PREFIX = "gcn.circulars"

    def __init__(
        self,
        config,
        send_to_slack: bool = True,
        include_test_topics: bool = False,
    ) -> None:
        from config import BotConfig
        from slack.client import SlackManager
        from slack.formatters import MessageFormatter
        from slack.too_handler import SlackToOHandler
        from slack.too_slot import ToOSlotStore
        from gcn.notice_handler import GCNNoticeHandler
        from gcn.circular_handler import GCNCircularHandler
        from gcn.consumer import GCNConsumer
        from too.emailer import GCNToOEmailer
        from visibility.plotter import VisibilityManager
        from visibility.tiles import TileManager

        self.config = config
        self.send_to_slack = send_to_slack
        self.include_test_topics = include_test_topics

        # --- Slack ---
        self.slack = SlackManager(
            token=config.slack_token,
            channel=config.slack_channel,
            app_token=config.slack_app_token,
        )
        self.formatter = MessageFormatter()
        # Per-event 'Auto-ToO' status message (edited in place); see slack/too_slot.py
        self.too_slot_store = ToOSlotStore(config.output_too_slots)
        self._too_slots: dict = {}
        self._too_slots_lock = threading.Lock()
        # telescope -> SMTP pre-warm lease taken for the notice being processed
        # (main thread only; see _prewarm_auto_too / _process_notice_message)
        self._notice_leases: dict = {}

        # --- Notice / Circular storage ---
        self.notice_handler = GCNNoticeHandler(
            output_csv=config.output_notice_csv,
            output_ascii=config.output_ascii,
            ascii_max_events=config.ascii_max_events,
            thread_index_path=config.output_thread_index,
            thread_index_max_age_days=config.thread_index_max_age_days,
        )
        self.circular_handler = GCNCircularHandler(
            output_csv=config.output_circular_csv,
            output_ascii=config.output_ascii,
            ascii_max_events=config.ascii_max_events,
            file_lock=self.notice_handler.file_lock,  # same ASCII file → same lock
        )
        # Same-burst linking across facility families (criteria: gcn/linking.py)
        self.link_params = linking.LinkParams.from_config(config)

        # --- Visibility ---
        self.visibility = VisibilityManager(
            min_altitude=config.min_altitude,
            min_moon_sep=config.min_moon_sep,
        )

        # --- Tile coverage ---
        # 7DT uses the default supy tile file; RASA36 uses its own tile file.
        self.tiles = TileManager(
            enable_db_check=config.turn_on_db_tile_check,
            label="7DT",
            gwportal_base_url=config.gwportal_base_url,
            gwportal_api_key=config.gwportal_api_key,
        )
        import os as _os
        _here = _os.path.dirname(_os.path.abspath(__file__))
        _rasa36_tile_path = _os.path.abspath(
            _os.path.join(_here, "supy", "supy", "refdata", "tileinfo", "RASA36", "final_tiles.txt")
        )
        self.tiles_rasa36 = TileManager(
            tile_path=_rasa36_tile_path,
            enable_db_check=False,          # DB reference-image check is 7DT-specific
            label="RASA36",
        )

        # --- ToO email ---
        # Both telescopes auto-ToO for GRBs localized to 1..MAX_AUTO_TOO_TILES (5)
        # tiles — the single-tile and multi-tile(<=5) cases share one config per
        # telescope. 7DT uses its emailer defaults (100 s x 3, Spec); RASA36 uses
        # the rapid config below.
        self.emailer = GCNToOEmailer(
            email_from=config.email_from,
            email_to=config.email_to,
            email_password=config.email_password,
            telescope="7DT",
            auto_too_grb_only=config.too_auto_grb_only,
            auto_too_max_tiles=config.auto_too_max_tiles,
            observable_soon_hours=config.too_observable_soon_hours,
            smtp_prewarm=config.smtp_prewarm,
        )
        self.emailer_rasa36 = GCNToOEmailer(
            email_from=config.email_from,
            email_to=config.email_to_rasa36,
            email_password=config.email_password,
            telescope="RASA36",
            auto_too_grb_only=config.too_auto_grb_only,
            auto_too_max_tiles=config.auto_too_max_tiles,
            observable_soon_hours=config.too_observable_soon_hours,
            smtp_prewarm=config.smtp_prewarm,
        )
        # Auto-ToO observation params (from settings.toml), split by tile case:
        # the single-tile (n==1) and multi-tile (2..max) cases are handled
        # separately, each with its own config per telescope. Selected at
        # send/fire time by _auto_too_config(telescope, n_tiles).
        self._auto_too_configs = {
            "7DT": {
                "single": config.too_config_7dt_auto_single,
                "multi":  config.too_config_7dt_auto_multi,
            },
            "RASA36": {
                "single": config.too_config_rasa36_auto_single,
                "multi":  config.too_config_rasa36_auto_multi,
            },
        }
        # Per-telescope × per-case enable flags (fully independent on/off).
        self._auto_too_enabled_flags = {
            "7DT": {
                "single": config.turn_on_too_email_auto_7dt_single,
                "multi":  config.turn_on_too_email_auto_7dt_multi,
            },
            "RASA36": {
                "single": config.turn_on_too_email_auto_rasa36_single,
                "multi":  config.turn_on_too_email_auto_rasa36_multi,
            },
        }

        # --- Deferred ToO scheduler ---
        # Fires "observable soon" ToOs once the target is actually up, after a
        # final retraction/coordinate/tile re-check. Started in run().
        self.deferred_scheduler = DeferredToOScheduler(
            on_fire=self._fire_deferred_too,
            check_interval=config.too_deferred_check_interval_sec,
        )

        # --- ToO observation tracking ---
        # Reads the 7DT / RASA36 Slack alert channels and appends accepted /
        # observed / not-announced lines to the event's ToO slot. Started in run().
        self.obs_tracker = None
        if config.obs_track_enabled and any(config.obs_alert_channels.values()):
            self.obs_tracker = ObservationTracker(
                client=self.slack.client,
                channels=config.obs_alert_channels,
                store_path=config.output_too_tracking,
                on_line=self._on_obs_line,
                permalink=self.slack.get_permalink,
                fast_poll_sec=config.obs_track_fast_poll_sec,
                slow_poll_sec=config.obs_track_slow_poll_sec,
                observe_poll_sec=config.obs_track_observe_poll_sec,
                visibility=self._tracker_window,
                transfer_wait_hours=config.obs_track_transfer_wait_hours,
                receipt_timeout_min=config.obs_track_receipt_timeout_min,
                night_end_hour_utc=config.obs_track_night_end_utc_hour,
                max_days=config.obs_track_max_days,
                lookback_hours=config.obs_track_lookback_hours,
                sender=config.email_from,
            )

        # --- Slack ToO handlers (one per telescope) ---
        self.too_handler = SlackToOHandler(
            slack=self.slack,
            emailer=self.emailer,
            user_group=config.too_user_group,
            too_config=config.too_config,
            telescope="7DT",
            on_sent=self._on_manual_too_sent,
            sent_at_provider=self.notice_handler.too_sent_at,
            link_note_provider=self._too_link_note,
        )
        self.too_handler_rasa36 = SlackToOHandler(
            slack=self.slack,
            emailer=self.emailer_rasa36,
            user_group=config.too_user_group,
            too_config=config.too_config,
            telescope="RASA36",
            on_sent=self._on_manual_too_sent,
            sent_at_provider=self.notice_handler.too_sent_at,
            link_note_provider=self._too_link_note,
            defaults_provider=self._rasa36_form_config,
        )

        # --- Kafka consumer ---
        # Subscribe to notice topics + circular topic
        all_topics = list(config.display_topics) + [self.CIRCULAR_TOPIC_PREFIX]
        self.consumer = GCNConsumer(
            client_id=config.gcn_id,
            client_secret=config.gcn_secret,
            topics=all_topics,
            connection_timeout=config.connection_timeout,
        )

        _log_banner(
            "GCNBot ready",
            [
                f"Slack channel : {config.slack_channel}",
                f"Topics        : {len(all_topics)} subscribed",
                f"Telescopes    : 7DT, RASA36 (CTIO) + LOAO",
                f"Send to Slack : {send_to_slack}",
                f"Auto-ToO 7DT   : single={config.turn_on_too_email_auto_7dt_single} "
                f"multi={config.turn_on_too_email_auto_7dt_multi}",
                f"Auto-ToO RASA36: single={config.turn_on_too_email_auto_rasa36_single} "
                f"multi={config.turn_on_too_email_auto_rasa36_multi}",
                f"Same-burst link: {config.same_event_link} "
                f"(±{self.link_params.time_window_sec:g} s, {self.link_params.pos_sigma:g}σ)",
                f"ToO tracking  : "
                + (", ".join(self.obs_tracker.channels) if self.obs_tracker else "off"),
            ],
        )

    # ------------------------------------------------------------------
    # Core pipeline
    # ------------------------------------------------------------------

    def process_notice(self, topic: str, value: Any) -> None:
        """
        Route and process a single Kafka message.

        Called by :meth:`GCNConsumer.start` for every non-heartbeat message.
        Errors are caught internally so the polling loop never crashes.
        """
        try:
            logger.info("=" * 55)
            logger.info(f"Processing message from: {topic}")

            # Route GCN Circulars separately
            if topic.startswith(self.CIRCULAR_TOPIC_PREFIX):
                self._process_circular(topic, value)
                return

            # Skip _TEST topics unless explicitly included
            if "_TEST" in topic.upper() and not self.include_test_topics:
                logger.info(f"Skipping test topic: {topic}")
                return

            self._process_notice_message(topic, value)

        except Exception as exc:
            logger.error(f"Unhandled error processing {topic}: {exc}", exc_info=True)

    # ------------------------------------------------------------------
    # Internal: circular routing
    # ------------------------------------------------------------------

    def _process_circular(self, topic: str, value: Any) -> None:
        """Route a Kafka circular message to the circular handler, then notify Slack."""
        import json as _json
        try:
            raw = value.decode("utf-8") if isinstance(value, bytes) else str(value)
            try:
                data = _json.loads(raw)
            except _json.JSONDecodeError:
                data = {"body": raw, "subject": "", "circularId": None, "createdOn": 0}

            # Parse + persist (returns None on hard failure)
            processed = self.circular_handler.process_circular_from_json(data)
            if processed and self.send_to_slack:
                self._handle_circular_slack(processed)

        except Exception as exc:
            logger.error(f"Error processing circular from {topic}: {exc}", exc_info=True)

    def _thread_from_index(self, processed: dict, event_name: Optional[str]) -> "tuple[str, str]":
        """Look a circular's event up in the thread index; ``(thread_ts, via)`` or ``("", "")``.

        Order: facility + trigger, event name, then (for "EP Alert YYMMDD.ff" /
        "SVOM GRB YYMMDD.ff" circulars) the event at that trigger time. A name hit is refused when the
        circular explicitly names a *different* trigger of the same facility
        family (the bot letters events in the order it sees them, so its "GRB
        261029C" need not be GCN's).
        """
        nh = self.notice_handler
        facility    = processed.get("facility")
        trigger_num = processed.get("trigger_num")
        try:
            if facility and trigger_num:
                entry = nh.lookup_thread(facility, trigger_num)
                if entry:
                    return entry["thread_ts"], "thread index (trigger)"
            if event_name:
                entry = nh.lookup_thread_by_name(event_name)
                if entry:
                    fam, _, idx_trigger = entry["key"].partition("|")
                    if (facility and trigger_num
                            and nh._normalize_facility(facility) == fam
                            and str(trigger_num).strip() != idx_trigger):
                        logger.info(
                            f"Circular {processed.get('circular_id')}: name {event_name!r} is in "
                            f"the thread index as {fam} #{idx_trigger}, but the circular cites "
                            f"#{trigger_num} — not used"
                        )
                    else:
                        return entry["thread_ts"], "thread index (name)"
            ep_time = processed.get("ep_alert_time")
            if ep_time:
                hit = nh.find_ep_thread_by_time(
                    ep_time, float(self.config.circular_ep_match_minutes)
                )
                if hit:
                    return hit["thread_ts"], f"EP time match ({hit['name']}, Δ{hit['delta_min']} min)"
            grb_time = processed.get("grb_alert_time")
            if grb_time:
                family = processed.get("grb_alert_family")
                hit = nh.find_thread_by_time(
                    grb_time, float(self.config.circular_ep_match_minutes),
                    {family} if family else set(self.link_params.families),
                )
                if hit:
                    return hit["thread_ts"], f"GRB time match ({hit['name']}, Δ{hit['delta_min']} min)"
        except Exception as exc:
            logger.error(f"Thread-index lookup for circular failed: {exc}")
        return "", ""

    def _handle_circular_slack(self, processed: dict) -> None:
        """Post a message to the matching Slack thread for a processed circular.

        Decision tree
        -------------
        Re-published, identical → silent (``processed["dedup"] == "duplicate"``)
        Re-published, edited    → :pencil2: reply, only if the subject changed or
                                  it newly reports a false trigger
        False trigger  → :warning: thread reply
        First circular → :bell: thread reply with event-page URL
        Follow-up      → :memo: thread reply (subject hyperlink only)
        No match       → silent skip
        """
        event_id    = processed.get("event_id")
        facility    = processed.get("facility")
        trigger_num = processed.get("trigger_num")
        is_first    = processed.get("is_first_circular", False)
        is_false    = processed.get("false_trigger", False)
        circular_id = processed.get("circular_id")
        subject     = processed.get("subject", "")
        event_page  = processed.get("event_page_url")

        if not circular_id or not subject:
            return

        # ---- GCN re-publishes: identical copies are silent; edits only when visible ----
        dedup = processed.get("dedup", "new")
        if dedup == "duplicate":
            return
        if dedup == "edited":
            subject_changed = processed.get("prev_subject", "") != subject
            newly_false     = is_false and not processed.get("prev_false_trigger", False)
            if not (subject_changed or newly_false):
                logger.info(
                    f"Circular {circular_id}: edited without a subject / false-trigger "
                    "change — databases updated, no Slack post"
                )
                return

        # ---- Find the matching ASCII event (two passes) ----
        ascii_event: Optional[dict] = None

        # Event name: GCN's official eventId, else the name parsed from the subject.
        event_name = event_id or processed.get("event_name")

        # Pass 1 — match by event name in the Name column (space-insensitive)
        if event_name:
            try:
                ascii_event = self.notice_handler.find_event_by_name(event_name)
            except Exception as exc:
                logger.error(f"find_event_by_name failed: {exc}")

        # Pass 2 — match by facility + trigger_num
        if ascii_event is None and facility and trigger_num:
            try:
                # A provisional name that differs from GCN's eventId (e.g. the
                # bot's "EP 260527b" vs the official "EP260527a" after a letter
                # was reused) is corrected below, by _official_renames.
                ascii_event = self.notice_handler.find_existing_event(
                    facility, trigger_num, return_full_data=True
                )
            except Exception as exc:
                logger.error(f"find_existing_event failed: {exc}")

        thread_ts = ""
        via = ""
        if ascii_event is not None:
            thread_ts = str(ascii_event.get("thread_ts", "")).strip().strip('"')
            via = "ASCII"

        # Pass 3 — the thread index (30 d) when the ASCII file (10 rows) has no
        # usable row: the event was trimmed out, or a circular re-created its row
        # without a thread_ts. Thread-only: ASCII row fixes (renames) are skipped.
        if not thread_ts:
            thread_ts, via = self._thread_from_index(processed, event_name)
            if thread_ts:
                ascii_event = None

        if not thread_ts:
            reason = "no ASCII match" if ascii_event is None else "ASCII row has no thread_ts"
            logger.info(
                f"Circular {circular_id}: {reason} and not in the thread index "
                f"(event={event_name!r}, facility={facility!r}, trigger={trigger_num!r}) "
                "— skipping Slack post"
            )
            return
        logger.info(f"Circular {circular_id}: matched thread {thread_ts} via {via}")

        # ---- One burst, several facilities: link what the circular names, give
        # the group GCN's name, and post to the canonical (first) thread ----
        nh = self.notice_handler
        anchor = nh.key_for_thread(thread_ts)
        joined: List[dict] = []
        confirmed: List[dict] = []
        if self.config.same_event_link:
            try:
                joined, confirmed = self._link_from_circular(processed)
            except Exception as exc:
                logger.error(f"Circular {circular_id}: same-burst linking failed: {exc}", exc_info=True)
        try:
            renamed = self._official_renames(anchor, event_id, via, {j["key"] for j in joined})
            if anchor:
                root = nh.group_root(anchor)
                canon_ts = (nh.lookup_thread(*root.split("|", 1)) or {}).get("thread_ts", "")
                if canon_ts and canon_ts != thread_ts:
                    logger.info(f"Circular {circular_id}: {anchor} is linked to {root} — "
                                f"posting to its thread {canon_ts}")
                    thread_ts = canon_ts
            self._post_circular_link_notes(circular_id, thread_ts, joined, confirmed, renamed)
        except Exception as exc:   # never let this block the circular post itself
            logger.error(f"Circular {circular_id}: rename / link notes failed: {exc}", exc_info=True)

        # ---- Build and send the circular thread message ----
        circular_url = f"https://gcn.nasa.gov/circulars/{circular_id}"
        link = f"<{circular_url}|{subject}>"

        if dedup == "edited":
            text = f":pencil2: *Circular updated* — {link}"
            if is_false and not processed.get("prev_false_trigger", False):
                text += "\n:warning: Now reports a *false trigger*."
        elif is_false:
            text = f":warning: *False trigger* — {link}"
        elif is_first and event_page:
            # Named per facility: a linked burst's canonical thread gets one per facility.
            family = linking.family_of(facility)
            label = f"First {family} circular" if family else "First circular"
            text = f":bell: {label}: {link}\nEvent page: <{event_page}|{event_page}>"
        else:
            text = f":memo: {link}"

        resp = self.slack.send_thread_message(thread_ts=thread_ts, text=text)
        if resp:
            logger.info(f"Circular {circular_id} posted to thread {thread_ts}")
        else:
            logger.error(f"Failed to post circular {circular_id} to Slack thread {thread_ts}")

    def _link_from_circular(self, processed: dict) -> "tuple[List[dict], List[dict]]":
        """Link the events a circular explicitly names — trigger IDs in its text
        or the triggers of circulars it cites.

        Only explicitly named events are paired, never the event the circular
        was matched to by *name*: the bot letters events in its own order, so its
        "GRB 261010A" can be a different GBM trigger than GCN's. Two named events
        of different families join one group unless their triggers are more than
        ``circular_max_dt_hours`` apart or their positions contradict (circulars
        also cite *other* bursts — 35–95 h apart in the Sep–Nov 2026 history).
        Returns ``(joined, confirmed)``: members newly linked, and members whose
        notice-time link this circular confirms.
        """
        nh, p = self.notice_handler, self.link_params
        cid = processed.get("circular_id")
        named = {}
        for item in processed.get("mentioned") or []:
            fam, _, trig = item["key"].partition("|")
            if fam in p.families and nh.lookup_thread(fam, trig) is not None:
                named.setdefault(item["key"], item["via"])
        if len(named) < 2:
            return [], []
        info = {k: next((m for m in nh.group_members(k) if m["key"] == k), None) for k in named}
        joined: List[dict] = []
        keys = list(named)
        for i, a_key in enumerate(keys):
            for b_key in keys[i + 1:]:
                a, b = info[a_key], info[b_key]
                if a is None or b is None or a["family"] == b["family"]:
                    continue
                if nh.group_root(a_key) == nh.group_root(b_key):
                    continue
                veto = self._circular_link_veto(a, b)
                if veto:
                    logger.info(f"Circular {cid} names {a_key} and {b_key} — not linked: {veto}")
                    continue
                dt = (b["t"] - a["t"]).total_seconds()
                root = nh.link_events(b_key, a_key, {"rule": "circular", "circular": cid,
                                                     "via": f"{named[a_key]} + {named[b_key]}",
                                                     "dt_s": round(dt, 1)})
                if root:
                    # report the member(s) that were not already in the canonical group
                    for k, m in ((a_key, a), (b_key, b)):
                        if k != root and not any(j["key"] == k for j in joined):
                            joined.append(m)
        # A circular naming both an earlier-linked member and its canonical event
        # confirms that (notice-time) link.
        confirmed: List[dict] = []
        joined_keys = {j["key"] for j in joined}
        for key in keys:
            m = info[key]
            root = nh.group_root(key)
            if m is None or key == root or root not in named or key in joined_keys:
                continue
            link = (nh.lookup_thread(m["family"], m["trigger"]) or {}).get("link") or {}
            if link.get("rule") in ("time", "position") and not link.get("confirmed_by"):
                nh.update_link_info(key, confirmed_by=cid)
                confirmed.append(m)
        return joined, confirmed

    def _circular_link_veto(self, a: dict, b: dict) -> Optional[str]:
        """Why two events a circular names together are *not* the same burst."""
        if a.get("t") is None or b.get("t") is None:
            return "trigger time unknown"
        dt = abs((b["t"] - a["t"]).total_seconds())
        if dt > self.link_params.circular_max_dt_hours * 3600:
            return f"triggers {dt / 3600:.1f} h apart (> {self.link_params.circular_max_dt_hours:g} h)"
        pos = linking.position_check(a, b, self.link_params)
        if pos and not pos[0]:
            return f"positions disagree ({pos[1]:.1f}° > {pos[2]:.1f}° allowed)"
        return None

    def _official_renames(
        self, anchor: Optional[str], event_id: Optional[str], via: str, joined_keys: set,
    ) -> dict:
        """Give GCN's eventId to the burst's events that carry another name.

        Applies to events this circular just linked, and to the whole group when
        the circular matched an ASCII row whose own name differs from GCN's (the
        provisional-name correction — a matched row that already carries the
        name may be a different event GCN lettered the same, so it renames
        nothing beyond what was just linked). Same prefix only (an "EP …" name
        stays). Returns ``{key: old_name}`` for visible renames
        (whitespace/case-only fixes are silent).
        """
        if not (anchor and event_id):
            return {}
        nh = self.notice_handler
        target = str(event_id).strip()
        anchor_name = (nh.lookup_thread(*anchor.split("|", 1)) or {}).get("name", "")
        whole_group = via == "ASCII" and anchor_name.strip() != target
        renamed: dict = {}
        for m in nh.group_members(anchor):
            if not whole_group and m["key"] not in joined_keys:
                continue
            if linking.name_prefix(m["name"]) != linking.name_prefix(target):
                continue
            old = (nh.lookup_thread(m["family"], m["trigger"]) or {}).get("name") or m["name"]
            if old == target and m["name"] == target:
                continue
            nh.rename_event(m["family"], m["trigger"], target)
            if nh._name_key(old) != nh._name_key(target):
                renamed[m["key"]] = old
                logger.info(f"Renamed {m['key']} '{old}' → '{target}' (official GCN event ID)")
        return renamed

    def _post_circular_link_notes(
        self, circular_id: Any, canon_ts: str, joined: List[dict], confirmed: List[dict], renamed: dict,
    ) -> None:
        """Slack notes for renames and circular-made / confirmed links."""
        if not self.send_to_slack:
            return
        nh = self.notice_handler
        joined_keys = {j["key"] for j in joined}
        canon_key = nh.key_for_thread(canon_ts) if canon_ts else None
        canon_name = ""
        if canon_key:
            canon_name = (nh.lookup_thread(*canon_key.split("|", 1)) or {}).get("name", "")
        canon_url = self.slack.get_permalink(canon_ts) if (canon_ts and (joined or confirmed)) else ""
        for key, old in renamed.items():
            if key in joined_keys:
                continue
            ts = (nh.lookup_thread(*key.split("|", 1)) or {}).get("thread_ts")
            new = (nh.lookup_thread(*key.split("|", 1)) or {}).get("name", "")
            if ts:
                self.slack.send_thread_message(
                    thread_ts=ts, text=f":pencil: Event renamed: *{old}* → *{new}* (official GCN event ID)")
        for m in joined:
            m_ts = m.get("thread_ts")
            if m_ts:
                self.slack.send_thread_message(thread_ts=m_ts, text=self.formatter.same_burst_joined(
                    canon_name, canon_url, circular_id, renamed.get(m["key"])))
            if canon_ts:
                m_url = self.slack.get_permalink(m_ts) if m_ts else ""
                self.slack.send_thread_message(thread_ts=canon_ts, text=self.formatter.same_burst_joined_canonical(
                    {**m, "name": canon_name or m["name"]}, m_url, circular_id))
            logger.info(f"Circular {circular_id}: linked {m['key']} to {canon_key}")
        for m in confirmed:
            if m.get("thread_ts"):
                self.slack.send_thread_message(
                    thread_ts=m["thread_ts"],
                    text=(f":white_check_mark: Same burst as {canon_name} — confirmed by "
                          f"<https://gcn.nasa.gov/circulars/{circular_id}|GCN {circular_id}>."))

    # ------------------------------------------------------------------
    # Internal: notice pipeline
    # ------------------------------------------------------------------

    def _process_notice_message(self, topic: str, value: Any) -> None:
        """Run the notice pipeline; give back any SMTP pre-warm lease it left."""
        self._notice_leases = {}
        # when the bot got this notice — start of the auto-ToO timing line
        self._notice_received_at = time.time()
        try:
            self._process_notice_pipeline(topic, value)
        finally:
            leases, self._notice_leases = self._notice_leases, {}
            for telescope, token in leases.items():
                emailer, _ = self._telescope_ctx(telescope)
                if emailer is not None:
                    emailer.release_prewarm(token)       # non-blocking

    def _prewarm_auto_too(self, notice_data: dict) -> None:
        """Pre-warm the SMTP connection of each telescope that may auto-ToO this notice.

        Runs right after parsing, so the ~1.1-1.5 s login overlaps the ~3 s of
        visibility / tile work before the decision. Cheap gates only (the real
        decision needs the tile count): Slack/auto mode on, the telescope has an
        auto-ToO case enabled, GRB-only satisfied, an error radius present, and
        no ToO sent for the event yet. Leases are taken back in
        ``_process_notice_message`` unless a send consumed them.
        """
        if not self.send_to_slack:
            return
        from too.emailer import _is_grb_facility
        facility = str(notice_data.get("Facility", "")).strip()
        trigger  = str(notice_data.get("Trigger_num", "")).strip()
        error    = notice_data.get("Error")
        try:
            if error is None or not float(error) > 0:
                return
        except (TypeError, ValueError):
            return
        for telescope, emailer in (("7DT", self.emailer), ("RASA36", self.emailer_rasa36)):
            flags = self._auto_too_enabled_flags.get(telescope, {})
            if not (emailer.smtp_prewarm and (flags.get("single") or flags.get("multi"))):
                continue
            if emailer.auto_too_grb_only and not _is_grb_facility(facility):
                continue
            if facility and trigger and self.notice_handler.was_too_sent(facility, trigger, telescope):
                continue
            token = f"notice:{telescope}:{facility}|{trigger}:{uuid.uuid4().hex[:8]}"
            emailer.prewarm(token, emailer.PREWARM_HOLD_AUTO_SEC)
            self._notice_leases[telescope] = token

    def _process_notice_pipeline(self, topic: str, value: Any) -> None:
        """
        Full notice pipeline:

        1. Parse
        2. Find existing event (update vs. new)
        3. Visibility analysis
        4. Storage (CSV + ASCII)
        5a. Existing event → thread update
        5b. New event → main message + ToO
        """
        # 1. Parse
        notice_data = self.notice_handler.parse_notice(value, topic)
        if not notice_data:
            logger.warning(f"Could not parse notice from {topic}")
            return

        facility    = notice_data.get("Facility", "")
        trigger_num = notice_data.get("Trigger_num", "")
        self._prewarm_auto_too(notice_data)
        name        = notice_data.get("Name", "GRB Candidate")
        ra          = notice_data.get("RA")
        dec         = notice_data.get("DEC")

        # 2. Check for existing event
        existing_event    = None
        existing_thread_ts: Optional[str] = None
        is_update = False

        if facility and trigger_num:
            try:
                existing_event = self.notice_handler.find_existing_event(
                    facility, trigger_num, return_full_data=True
                )
                if existing_event and isinstance(existing_event, dict):
                    existing_thread_ts = existing_event.get("thread_ts", "")
                    logger.info(
                        f"Existing event found for {facility} #{trigger_num} "
                        f"(thread_ts={existing_thread_ts})"
                    )
                if not existing_thread_ts:
                    # Row evicted from the 10-row ASCII file, or re-created by a
                    # circular without a thread_ts — recover the original thread.
                    indexed = self.notice_handler.lookup_thread(facility, trigger_num)
                    if indexed:
                        existing_thread_ts = indexed["thread_ts"]
                        existing_event = existing_event if isinstance(existing_event, dict) else {}
                        logger.info(
                            f"Recovered thread_ts={existing_thread_ts} from thread index "
                            f"for {facility} #{trigger_num}"
                        )
                is_update = bool(existing_thread_ts)
            except Exception as exc:
                logger.error(f"Error looking up existing event: {exc}")

        # 2b. A new event may be a burst another facility already reported
        # (Fermi GBM + SVOM 2 s apart): link it, and reuse that event's name.
        link: Optional[dict] = None
        if not is_update and self.config.same_event_link:
            try:
                link = self.notice_handler.find_same_event(notice_data, self.link_params)
            except Exception as exc:
                logger.error(f"Same-burst lookup failed: {exc}")

        # 3. Visibility — 7DT (Chile) and LOAO (Arizona)
        visibility_result      = None
        visibility_result_loao = None
        if ra is not None and dec is not None:
            try:
                visibility_result = self.visibility.get_status(ra=ra, dec=dec)
                if visibility_result:
                    logger.info(
                        f"7DT visibility for {facility} #{trigger_num}: {visibility_result.get('case')}"
                    )
            except Exception as exc:
                logger.error(f"7DT visibility analysis failed: {exc}")
            try:
                visibility_result_loao = self.visibility.get_status_loao(ra=ra, dec=dec)
                if visibility_result_loao:
                    logger.info(
                        f"LOAO visibility for {facility} #{trigger_num}: {visibility_result_loao.get('case')}"
                    )
            except Exception as exc:
                logger.error(f"LOAO visibility analysis failed: {exc}")

        # 3b. Tile coverage — 7DT and RASA36
        tile_result = None
        tile_result_rasa36 = None
        if ra is not None and dec is not None:
            try:
                error = notice_data.get("Error")
                if error is not None and float(error) > 0:
                    tile_result = self.tiles.get_tile_info(ra=ra, dec=dec, error=float(error))
                    if tile_result:
                        logger.info(f"7DT tile coverage for {facility} #{trigger_num}: {tile_result['n_tiles']} tile(s)")
                    tile_result_rasa36 = self.tiles_rasa36.get_tile_info(ra=ra, dec=dec, error=float(error))
                    if tile_result_rasa36:
                        logger.info(f"RASA36 tile coverage for {facility} #{trigger_num}: {tile_result_rasa36['n_tiles']} tile(s)")
            except (ValueError, TypeError, Exception) as exc:
                logger.error(f"Tile coverage analysis failed: {exc}")

        # 4. Storage
        csv_status   = False
        ascii_status = False

        if link and not notice_data.get("Name") and (
            linking.name_prefix(link["name"])
            == linking.facility_prefix(linking.family_of(facility))
        ):
            # Same burst, same name — no new letter (EP ↔ GRB names stay apart).
            notice_data["Name"] = link["name"]
        self.notice_handler.assign_name(notice_data)

        if self.config.turn_on_notice:
            try:
                csv_status = self.notice_handler.save_to_csv(notice_data)
            except Exception as exc:
                logger.error(f"CSV save failed: {exc}")

        # 5a. Update path
        if is_update and existing_thread_ts:
            self._handle_update(
                topic, value, notice_data,
                existing_event, existing_thread_ts,
                visibility_result, visibility_result_loao, ra, dec,
                tile_result=tile_result,
                tile_result_rasa36=tile_result_rasa36,
            )
            return

        # 5b. New-event path
        self._handle_new_event(
            topic, value, notice_data,
            csv_status, visibility_result, visibility_result_loao, ra, dec,
            tile_result=tile_result,
            tile_result_rasa36=tile_result_rasa36,
            link=link,
        )

    # ------------------------------------------------------------------
    # Internal: update path
    # ------------------------------------------------------------------

    def _handle_update(
        self,
        topic: str,
        value: Any,
        notice_data: dict,
        existing_event: dict,
        existing_thread_ts: str,
        visibility_result:      Optional[dict],
        visibility_result_loao: Optional[dict],
        ra:  Optional[float],
        dec: Optional[float],
        tile_result: Optional[dict] = None,
        tile_result_rasa36: Optional[dict] = None,
    ) -> None:
        """Post a thread reply when an existing event is updated."""
        # Auto-ToO on refined coordinates: a position update may newly qualify
        # (e.g. a wide GBM box shrinking to a single tile once Swift/EP refines
        # it). Gated by turn_on_too_email_auto_on_update; was_too_sent inside
        # _dispatch_auto_too prevents a second send if this event was already
        # ToO'd. The event's thread already exists, so feed it straight into the
        # event's status slot for immediate threading.
        too_dispatched: List[str] = []
        if self.config.turn_on_too_email_auto_on_update:
            too_slot = self._too_slot_for_thread(existing_thread_ts)
            too_dispatched = self._dispatch_auto_too(
                notice_data, visibility_result, tile_result, tile_result_rasa36, too_slot
            )

        # Save ASCII with the existing thread_ts (and any ToO-sent marker) so the
        # row is refreshed.
        try:
            self.notice_handler.save_to_ascii(
                notice_data, existing_thread_ts, too_sent=too_dispatched,
                too_tiles=self._dispatched_tiles(too_dispatched, tile_result, tile_result_rasa36),
            )
        except Exception as exc:
            logger.error(f"ASCII save (update) failed: {exc}")

        # Compare to find meaningful changes
        differences = self.notice_handler.compare_event_data(existing_event, notice_data)
        if not differences:
            logger.info("No meaningful differences found — skipping thread update")
            return

        # Plots are re-uploaded only when THREAD_UPDATE_PLOTS is on: the text
        # carries position + shift, tile counts and observability instead (each
        # update used to add 3-5 posts; GBM FIN_POS alone ~20 updates/month).
        coord_changed = isinstance(differences, dict) and "coordinates" in differences
        plots = coord_changed and self.config.thread_update_plots

        thread_update = self.formatter.format_thread_update(
            differences, notice_data, kind=self.formatter.notice_kind(topic),
            previous=existing_event,
            tile_result=tile_result, tile_result_rasa36=tile_result_rasa36,
            visibility_result=visibility_result, visibility_result_loao=visibility_result_loao,
        )
        send_kwargs = dict(thread_ts=existing_thread_ts, text=str(thread_update))

        if self.send_to_slack:
            # Upload tile plots first (so they appear above the text in the thread)
            if plots and ra is not None and dec is not None:
                error = notice_data.get("Error")
                if tile_result is not None:
                    try:
                        if error is not None and float(error) > 0:
                            tile_title = f"7DT Tile - RA: {ra} / Dec: {dec} / Error: {error}"
                            tile_buf = self.tiles.generate_plot(
                                ra=ra, dec=dec, error=float(error), title=tile_title
                            )
                            if tile_buf:
                                self.slack.upload_file(
                                    thread_ts=existing_thread_ts,
                                    file=tile_buf,
                                    filename="tiles_7dt_update.png",
                                    title=tile_title,
                                )
                    except Exception as exc:
                        logger.error(f"Failed to upload updated 7DT tile plot: {exc}")
                if tile_result_rasa36 is not None:
                    try:
                        if error is not None and float(error) > 0:
                            tile_title = f"RASA36 Tile - RA: {ra} / Dec: {dec} / Error: {error}"
                            tile_buf = self.tiles_rasa36.generate_plot(
                                ra=ra, dec=dec, error=float(error), title=tile_title
                            )
                            if tile_buf:
                                self.slack.upload_file(
                                    thread_ts=existing_thread_ts,
                                    file=tile_buf,
                                    filename="tiles_rasa36_update.png",
                                    title=tile_title,
                                )
                    except Exception as exc:
                        logger.error(f"Failed to upload updated RASA36 tile plot: {exc}")

            resp = self.slack.send_thread_message(**send_kwargs)
            if not resp:
                logger.error("Failed to post thread update to Slack")
                return

            logger.info(f"Thread update posted (thread_ts={existing_thread_ts})")

            # Upload updated visibility plots if coordinates changed
            if plots and ra is not None and dec is not None:
                target = notice_data.get("Name", "Target")
                if visibility_result:
                    self._upload_visibility_plot(
                        thread_ts=existing_thread_ts,
                        ra=ra, dec=dec,
                        title=f"Updated 7DT & RASA36 Visibility: {target}",
                        observatory="7DT",
                    )
                if visibility_result_loao:
                    self._upload_visibility_plot(
                        thread_ts=existing_thread_ts,
                        ra=ra, dec=dec,
                        title=f"Updated LOAO Visibility: {target}",
                        observatory="LOAO",
                    )

    # ------------------------------------------------------------------
    # Internal: new-event path
    # ------------------------------------------------------------------

    def _handle_new_event(
        self,
        topic: str,
        value: Any,
        notice_data: dict,
        csv_status: bool,
        visibility_result:      Optional[dict],
        visibility_result_loao: Optional[dict],
        ra:  Optional[float],
        dec: Optional[float],
        tile_result: Optional[dict] = None,
        tile_result_rasa36: Optional[dict] = None,
        link: Optional[dict] = None,
    ) -> None:
        """Post the initial Slack message for a brand-new event.

        *link* (from ``find_same_event``) marks the event as a burst another
        facility already reported: its auto-ToO is group-guarded, the message
        carries a SAME_BURST_AS line, and both threads get a cross-link.
        """
        # Dispatch the auto-ToO email FIRST, before any Slack work. The send is
        # async, so the email now fires in parallel with plot rendering/uploads
        # (which follow) instead of waiting ~5-15s behind them. Its inputs
        # (visibility_result + tile_result) are already computed. The success/
        # failure status goes into the event's 'Auto-ToO' slot, which accumulates
        # state silently and is posted by `too_slot.attach(thread_ts)` as soon as
        # the Slack post below returns (so it sits right under the main message,
        # ahead of the plot uploads).
        too_slot = ToOSlot(self.slack, self.too_slot_store)
        too_dispatched = self._dispatch_auto_too(
            notice_data, visibility_result, tile_result, tile_result_rasa36, too_slot,
            link=link,
        )

        # Save to ASCII first (without thread_ts) to get storage status. The
        # ToO-sent marker is folded in here — this write always runs (unlike the
        # thread_ts update below, which is skipped when the Slack post fails), so
        # the guard survives a Slack-failure re-entry of this path.
        ascii_status = False
        try:
            ascii_status = self.notice_handler.save_to_ascii(
                notice_data, too_sent=too_dispatched,
                too_tiles=self._dispatched_tiles(too_dispatched, tile_result, tile_result_rasa36),
            )
        except Exception as exc:
            logger.error(f"ASCII save (new event) failed: {exc}")

        # Same burst as an existing event: is this the best localization so far?
        link_best, link_best_member, canon_url = False, None, ""
        if link:
            link_best, link_best_member = self._best_in_group(notice_data, link["root"])
            if self.send_to_slack:
                canon_url = self.slack.get_permalink(link["thread_ts"])

        # Format main message.
        # format_notice returns (dict_or_None, lc_url, notice_url); extract blocks list.
        raw_result, lc_url, notice_url = self.formatter.format_notice(
            topic=topic,
            value=value,
            csv_status=csv_status,
            ascii_status=ascii_status,
            notice_data=notice_data,
            test_mode=self.include_test_topics,
            same_burst=(self.formatter.same_burst_header(link, link_best, canon_url)
                        if link else None),
        )
        if raw_result is None:
            logger.error("Message formatter returned None — aborting")
            too_slot.attach(None)  # no thread — flush any ToO status to the channel
            return
        blocks: list = raw_result.get("blocks", []) if isinstance(raw_result, dict) else list(raw_result)
        if not blocks:
            logger.error("Message formatter returned empty blocks — aborting")
            too_slot.attach(None)  # no thread — flush any ToO status to the channel
            return

        # Append visibility blocks (7DT + LOAO) directly to the main message
        vis_blocks = self.visibility.get_slack_blocks(
            visibility_result=visibility_result,
            visibility_result_loao=visibility_result_loao,
            lc_url=lc_url,
            notice_url=notice_url,
        )
        if vis_blocks:
            blocks.append({"type": "divider"})
            blocks.extend(vis_blocks)

        # Append tile coverage blocks (7DT + RASA36) to the main message
        tile_blocks = self.tiles.get_slack_blocks(tile_result)
        if tile_blocks:
            blocks.append({"type": "divider"})
            blocks.extend(tile_blocks)
        tile_blocks_rasa36 = self.tiles_rasa36.get_slack_blocks(tile_result_rasa36)
        if tile_blocks_rasa36:
            blocks.extend(tile_blocks_rasa36)

        # Add ToO buttons (7DT + RASA36)
        try:
            blocks = self.too_handler.add_too_button(blocks, notice_data)
        except Exception as exc:
            logger.warning(f"Could not add 7DT ToO button: {exc}")
        try:
            blocks = self.too_handler_rasa36.add_too_button(blocks, notice_data)
        except Exception as exc:
            logger.warning(f"Could not add RASA36 ToO button: {exc}")

        if not self.send_to_slack:
            logger.info("send_to_slack=False — skipping Slack post")
            too_slot.attach(None)  # no auto-ToO dispatched in this mode (no-op)
            return

        # Send main message
        resp = self.slack.send_message(
            blocks=blocks,
            text=f"GCN Alert: {notice_data.get('Name', 'New Target')}",
        )
        if not resp:
            logger.error("Failed to send Slack message")
            too_slot.attach(None)  # Slack post failed → channel fallback
            return

        new_thread_ts = resp.get("ts", "")
        # Bind the already-dispatched auto-ToO status slot to this thread (posts it
        # now, before the plot uploads).
        self._register_too_slot(new_thread_ts, too_slot)
        too_slot.attach(new_thread_ts)
        logger.info(
            f"New message posted (thread_ts={new_thread_ts}) for "
            f"{notice_data.get('Facility')} #{notice_data.get('Trigger_num')}"
        )

        # Update ASCII row with real thread_ts
        try:
            self.notice_handler.save_to_ascii(notice_data, new_thread_ts)
        except Exception as exc:
            logger.error(f"Failed to update ASCII with thread_ts: {exc}")

        # Same burst: record the link, then cross-link both threads (above the plots).
        if link:
            self._post_same_burst_link(
                notice_data, new_thread_ts, link, link_best, link_best_member, canon_url,
                tile_result, tile_result_rasa36, visibility_result,
            )

        # Upload visibility plots to thread (7DT+RASA36 share same site; then LOAO)
        target = notice_data.get("Name", "Target")
        if ra is not None and dec is not None:
            self._upload_visibility_plot(
                thread_ts=new_thread_ts,
                ra=ra, dec=dec,
                title=f"7DT & RASA36 Visibility: {target}",
                observatory="7DT",
            )
            self._upload_visibility_plot(
                thread_ts=new_thread_ts,
                ra=ra, dec=dec,
                title=f"LOAO Visibility: {target}",
                observatory="LOAO",
            )

        # Upload tile coverage plots to thread (7DT + RASA36)
        if ra is not None and dec is not None:
            error = notice_data.get("Error")
            if tile_result is not None:
                try:
                    if error is not None and float(error) > 0:
                        tile_title = f"7DT Tile - RA: {ra} / Dec: {dec} / Error: {error}"
                        tile_buf = self.tiles.generate_plot(
                            ra=ra, dec=dec, error=float(error), title=tile_title
                        )
                        if tile_buf:
                            self.slack.upload_file(
                                thread_ts=new_thread_ts,
                                file=tile_buf,
                                filename="tiles_7dt.png",
                                title=tile_title,
                            )
                except Exception as exc:
                    logger.error(f"Failed to upload 7DT tile plot: {exc}")
            if tile_result_rasa36 is not None:
                try:
                    if error is not None and float(error) > 0:
                        tile_title = f"RASA36 Tile - RA: {ra} / Dec: {dec} / Error: {error}"
                        tile_buf = self.tiles_rasa36.generate_plot(
                            ra=ra, dec=dec, error=float(error), title=tile_title
                        )
                        if tile_buf:
                            self.slack.upload_file(
                                thread_ts=new_thread_ts,
                                file=tile_buf,
                                filename="tiles_rasa36.png",
                                title=tile_title,
                            )
                except Exception as exc:
                    logger.error(f"Failed to upload RASA36 tile plot: {exc}")

        # NOTE: auto-ToO email is dispatched earlier (top of this method) so it
        # runs in parallel with the Slack work above, not serialized behind it.

    # ------------------------------------------------------------------
    # Internal: helpers
    # ------------------------------------------------------------------

    def _dispatch_auto_too(
        self,
        notice_data: dict,
        visibility_result: Optional[dict],
        tile_result: Optional[dict],
        tile_result_rasa36: Optional[dict],
        too_slot: "ToOSlot",
        link: Optional[dict] = None,
    ) -> List[str]:
        """
        Evaluate auto-ToO criteria and dispatch the (async) emails for both
        telescopes. Called at the top of both ``_handle_new_event`` and
        ``_handle_update`` (a refined position may newly qualify — e.g. Fermi
        GBM's wide error box shrinking to a single tile once Swift/EP refines
        it), so the send overlaps the Slack rendering/uploads that follow.

        Returns the list of telescope labels actually dispatched, so the caller
        can persist the ToO-sent marker in its own ``save_to_ascii`` write.

        Duplicate-send guard: each telescope is skipped when
        ``notice_handler.was_too_sent`` reports a prior send for this event, so
        an event ToO'd on its first notice is never re-sent on later updates
        (or on a Slack-failure re-entry of the new-event path).

        Same-burst guard: within a group of linked events only the best-localized
        one may auto-ToO, and not when a linked event already requested exactly
        the same tiles (see ``_group_too_block``; *link* names the group a brand-
        new event is about to join).

        Gated on ``send_to_slack`` to preserve prior behaviour: in dev mode
        without ``--send``, no automatic ToO emails go out. Each telescope×case
        combination has its own enable flag (checked inside
        ``_dispatch_auto_too_one`` once the tile case is known). Success/failure
        status lines go into the event's ``too_slot`` (one editable thread message).
        """
        dispatched: List[str] = []
        if not self.send_to_slack:
            return dispatched
        if self._dispatch_auto_too_one(
            "7DT", self.emailer, tile_result,
            notice_data, visibility_result, too_slot, link=link,
        ):
            dispatched.append("7DT")
        if self._dispatch_auto_too_one(
            "RASA36", self.emailer_rasa36, tile_result_rasa36,
            notice_data, visibility_result, too_slot, link=link,
        ):
            dispatched.append("RASA36")
        return dispatched

    def _dispatch_auto_too_one(
        self,
        telescope: str,
        emailer: "GCNToOEmailer",
        tile_result: Optional[dict],
        notice_data: dict,
        visibility_result: Optional[dict],
        too_slot: "ToOSlot",
        link: Optional[dict] = None,
    ) -> bool:
        """
        Handle one telescope's auto-ToO decision.

        Enable is per telescope × tile case (single vs multi); the case-specific
        flag is checked once the tile count is known. The obs config is likewise
        chosen by tile case via ``_auto_too_config``. Returns ``True`` only when
        an *immediate* ("now") send was dispatched — the caller folds that into
        the ASCII ToO-sent marker. An ``observable soon`` classification instead
        schedules a deferred send (persisted in ASCII, fired once the target is
        up, config re-selected then) and returns ``False``.
        """
        # Quick skip when neither tile case is enabled for this telescope.
        flags = self._auto_too_enabled_flags.get(telescope, {})
        if not (flags.get("single") or flags.get("multi")):
            return False

        facility    = notice_data.get("Facility", "")
        trigger     = notice_data.get("Trigger_num", "")
        key = (str(facility), str(trigger), telescope)
        try:
            if self.notice_handler.was_too_sent(facility, trigger, telescope):
                return False
            case = emailer.classify_auto_too(notice_data, visibility_result, tile_result)
        except Exception as exc:
            logger.error(f"Auto-ToO classify error ({telescope}): {exc}", exc_info=True)
            return False

        # Enforce the per-case enable flag now that the tile count is known.
        n_tiles = (tile_result or {}).get("n_tiles")
        if case is not None and not self._auto_too_enabled(telescope, n_tiles):
            tile_case = "single" if n_tiles == 1 else "multi"
            logger.info(
                f"Auto-ToO suppressed ({telescope}): {tile_case}-tile case disabled"
            )
            return False

        # Same burst seen by another facility: only its best localization ToOs,
        # and never the same tiles twice.
        if case is not None:
            blocked = self._group_too_block(telescope, notice_data, tile_result, link)
            if blocked:
                reason, visible = blocked
                logger.info(f"Auto-ToO suppressed ({telescope}): {reason}")
                if visible:
                    too_slot.add_line(telescope, f":link: not sent — {reason}")
                return False

        if case == "now":
            # A now-send supersedes any pending deferred entry for this event.
            if self.deferred_scheduler.is_pending(key):
                self.deferred_scheduler.cancel(key)
                self.notice_handler.clear_too_deferred(facility, trigger, telescope)
            too_config = self._auto_too_config(telescope, n_tiles)
            try:
                too_slot.begin_send(telescope)
                emailer.send_too_email_async(
                    notice_data,
                    too_config=too_config,
                    on_success=self._make_too_notice(
                        too_slot, telescope, True, notice_data, rapid=too_is_rapid(too_config),
                        notice_at=getattr(self, "_notice_received_at", None)),
                    on_failure=self._make_too_notice(too_slot, telescope, False, notice_data),
                    # the send gives the pre-warm lease back itself, after sending
                    release_token=self._notice_leases.pop(telescope, None),
                )
                return True
            except Exception as exc:
                logger.error(f"Auto-ToO email error ({telescope}): {exc}", exc_info=True)
            return False

        if case == "soon":
            if self.deferred_scheduler.is_pending(key):
                return False  # already scheduled — leave the existing fire time
            hrs = self._soon_hours(visibility_result)
            if hrs is None:
                return False
            fire_at = datetime.now(timezone.utc) + timedelta(hours=hrs)
            self.notice_handler.set_too_deferred(
                facility, trigger, telescope, fire_at.isoformat()
            )
            # Config is re-selected at fire time from the latest tile count, so
            # nothing case-specific needs to be baked into the entry.
            self.deferred_scheduler.schedule(DeferredEntry(
                facility=str(facility), trigger_num=str(trigger),
                telescope=telescope, fire_at=fire_at,
            ))
            too_slot.tag_deferred(telescope, f"{facility}|{trigger}")
            too_slot.add_line(telescope, self._scheduled_line(fire_at, rescheduled=False))
        return False

    # ------------------------------------------------------------------
    # Deferred ToO — fire-time re-check + send
    # ------------------------------------------------------------------

    def _telescope_ctx(self, telescope: str):
        """Return (emailer, tile_manager) for a telescope."""
        if telescope == "RASA36":
            return self.emailer_rasa36, self.tiles_rasa36
        if telescope == "7DT":
            return self.emailer, self.tiles
        return None, None

    def _auto_too_config(self, telescope: str, n_tiles: Optional[int]) -> Optional[dict]:
        """Pick the auto-ToO obs config for a telescope by tile case.

        ``n_tiles == 1`` → the single-tile config; ``2..max`` → the multi-tile
        config. The two cases are configured (and thus tunable) separately.
        """
        case = "single" if n_tiles == 1 else "multi"
        return self._auto_too_configs.get(telescope, {}).get(case)

    def _rasa36_form_config(self, notice_data: dict) -> Optional[dict]:
        """Auto-ToO config the RASA36 Slack form is pre-filled from.

        Mirrors the automatic path: the single- vs multi-tile config is chosen
        from the event's RASA36 tile count (computed from the button payload's
        RA/DEC/Error; unknown → same fallback as ``_auto_too_config``).
        """
        n_tiles = None
        try:
            ra, dec = float(notice_data["RA"]), float(notice_data["DEC"])
            error = float(notice_data.get("Error"))
            if error > 0:
                result = self.tiles_rasa36.get_tile_info(ra=ra, dec=dec, error=error)
                n_tiles = (result or {}).get("n_tiles")
        except (KeyError, TypeError, ValueError) as exc:
            logger.debug(f"RASA36 form: tile count unavailable ({exc})")
        return self._auto_too_config("RASA36", n_tiles)

    def _auto_too_enabled(self, telescope: str, n_tiles: Optional[int]) -> bool:
        """Whether auto-ToO is enabled for this telescope × tile case.

        ``n_tiles == 1`` → single-tile flag; ``2..max`` → multi-tile flag.
        """
        case = "single" if n_tiles == 1 else "multi"
        return bool(self._auto_too_enabled_flags.get(telescope, {}).get(case))

    @staticmethod
    def _soon_hours(visibility_result: Optional[dict]) -> Optional[float]:
        """Extract ``time_until_start_hours`` from a visibility result, or None."""
        if not visibility_result:
            return None
        window = (
            (visibility_result.get("details") or {})
            .get("tonight", {})
            .get("window", {})
        )
        hrs = window.get("time_until_start_hours")
        try:
            return float(hrs) if hrs is not None else None
        except (TypeError, ValueError):
            return None

    def _fire_deferred_too(self, entry: "DeferredEntry") -> None:
        """Pre-warm SMTP for the fire-time re-check, then run it (scheduler thread)."""
        emailer, _ = self._telescope_ctx(entry.telescope)
        token = f"deferred:{entry.telescope}:{entry.facility}|{entry.trigger_num}:{uuid.uuid4().hex[:8]}"
        if emailer is not None:
            emailer.prewarm(token, emailer.PREWARM_HOLD_AUTO_SEC)
        try:
            self._fire_deferred_too_checked(entry)
        finally:
            if emailer is not None:
                emailer.release_prewarm(token)

    def _fire_deferred_too_checked(self, entry: "DeferredEntry") -> None:
        """
        Fire (or re-evaluate) a deferred ToO — runs on the scheduler thread.

        Final re-check on the *latest* information before sending:
          1. ASCII row gone  → event retracted/false alarm → skip.
          2. Already sent     → skip.
          3. Recompute visibility + tiles on the row's latest coordinates and
             re-apply the gates. ``now`` → send (obs config re-selected from the
             latest tile count: single vs multi); ``soon`` → target rises a bit
             later than first estimated, reschedule; anything else (now outside
             the 1..max tile range, not observable) → skip.
        """
        fired_at = time.time()
        facility, trigger, telescope = entry.facility, entry.trigger_num, entry.telescope
        emailer, tiles = self._telescope_ctx(telescope)
        if emailer is None:
            logger.warning(f"Deferred ToO {entry.key}: unknown telescope — dropping")
            return

        row = self.notice_handler.find_existing_event(facility, trigger, return_full_data=True)
        if not row:
            logger.info(
                f"Deferred ToO {entry.key}: event no longer in ASCII "
                f"(retracted / trimmed) — skipping"
            )
            # The row (and its thread_ts) is gone; find the slot via its tag.
            slot_thread = self.too_slot_store.find_thread(telescope, f"{facility}|{trigger}")
            if slot_thread:
                self._post_deferred_result(
                    slot_thread, telescope, sent=None, reason="retracted / no longer qualifies"
                )
            return
        if self.notice_handler.was_too_sent(facility, trigger, telescope):
            logger.info(f"Deferred ToO {entry.key}: already sent — clearing")
            self.notice_handler.clear_too_deferred(facility, trigger, telescope)
            return

        name = str(row.get("Name", "")).strip().strip('"')
        thread_ts = str(row.get("thread_ts", "")).strip() or None
        try:
            ra  = float(row.get("RA"))
            dec = float(row.get("DEC"))
        except (TypeError, ValueError):
            logger.warning(f"Deferred ToO {entry.key}: unusable coordinates — skipping")
            self.notice_handler.clear_too_deferred(facility, trigger, telescope)
            self._post_deferred_result(
                thread_ts, telescope, sent=None, reason="unusable coordinates"
            )
            return
        error = row.get("Error")
        notice_data = {
            "Name": name, "RA": ra, "DEC": dec,
            "Facility": facility, "Trigger_num": trigger, "Error": error,
        }

        visibility_result = None
        try:
            visibility_result = self.visibility.get_status(ra=ra, dec=dec)
        except Exception as exc:
            logger.error(f"Deferred ToO {entry.key}: visibility recompute failed: {exc}")
        tile_result = None
        try:
            if error not in (None, "") and float(error) > 0:
                tile_result = tiles.get_tile_info(ra=ra, dec=dec, error=float(error))
        except Exception as exc:
            logger.error(f"Deferred ToO {entry.key}: tile recompute failed: {exc}")

        case = emailer.classify_auto_too(notice_data, visibility_result, tile_result)
        n_tiles = (tile_result or {}).get("n_tiles")

        # The tile case may have changed since scheduling (coords refined); if
        # the now-current case is disabled for this telescope, don't send.
        if case is not None and not self._auto_too_enabled(telescope, n_tiles):
            tile_case = "single" if n_tiles == 1 else "multi"
            logger.info(
                f"Deferred ToO {entry.key}: {tile_case}-tile case now disabled — skipping"
            )
            self.notice_handler.clear_too_deferred(facility, trigger, telescope)
            self._post_deferred_result(thread_ts, telescope, sent=None)
            return

        # A linked facility may have localized the burst better (or already
        # requested these tiles) while this ToO was waiting.
        if case is not None:
            blocked = self._group_too_block(telescope, notice_data, tile_result, None)
            if blocked:
                reason, _ = blocked
                logger.info(f"Deferred ToO {entry.key}: {reason} — skipping")
                self.notice_handler.clear_too_deferred(facility, trigger, telescope)
                self._post_deferred_result(thread_ts, telescope, sent=None, reason=reason)
                return

        if case == "now":
            too_config = self._auto_too_config(telescope, n_tiles)
            ok = emailer.send_too_email(notice_data, too_config)
            sent_at = time.time()
            if ok:
                self.notice_handler.mark_too_sent(
                    facility, trigger, telescope, tiles=(tile_result or {}).get("tile_ids"),
                )
                self._track_sent(telescope, facility, trigger, "deferred",
                                 notice_data.get("Name", ""), rapid=too_is_rapid(too_config),
                                 sent_at=sent_at)
            # Clear the pending marker either way; a failed send is not retried
            # (the operator is notified to submit manually).
            self.notice_handler.clear_too_deferred(facility, trigger, telescope)
            self._post_deferred_result(thread_ts, telescope, sent=ok,
                                       rapid=too_is_rapid(too_config),
                                       timing=self._timing(fired_at, sent_at, "fired"))
        elif case == "soon":
            hrs = self._soon_hours(visibility_result) or 0.05
            new_fire = datetime.now(timezone.utc) + timedelta(hours=hrs)
            self.notice_handler.set_too_deferred(
                facility, trigger, telescope, new_fire.isoformat()
            )
            self.deferred_scheduler.schedule(DeferredEntry(
                facility=facility, trigger_num=trigger, telescope=telescope,
                fire_at=new_fire, payload=entry.payload,
            ))
            self._too_slot_for_thread(thread_ts).add_line(
                telescope, self._scheduled_line(new_fire, rescheduled=True)
            )
            logger.info(
                f"Deferred ToO {entry.key}: target still rising — rescheduled to "
                f"{new_fire.isoformat()}"
            )
        else:
            logger.info(
                f"Deferred ToO {entry.key}: no longer qualifies (case={case}) — skipping"
            )
            self.notice_handler.clear_too_deferred(facility, trigger, telescope)
            self._post_deferred_result(thread_ts, telescope, sent=None)

    def _reload_deferred_too(self) -> None:
        """Re-schedule ToOs left pending in the ASCII file (called on startup).

        A fire time already in the past (bot was down when the target rose)
        schedules ~immediately so the fire-time re-check runs right away.
        """
        pending = self.notice_handler.get_pending_deferred()
        if not pending:
            return
        now = datetime.now(timezone.utc)
        for item in pending:
            try:
                fire_at = datetime.fromisoformat(item["fire_at"])
                if fire_at.tzinfo is None:
                    fire_at = fire_at.replace(tzinfo=timezone.utc)
            except (ValueError, KeyError):
                logger.warning(f"Deferred ToO reload: bad fire time for {item}")
                continue
            if fire_at < now:
                fire_at = now + timedelta(seconds=5)
            # Obs config is re-selected at fire time (by then-current tile count),
            # so nothing case-specific is baked into the entry.
            self.deferred_scheduler.schedule(DeferredEntry(
                facility=item["facility"], trigger_num=item["trigger_num"],
                telescope=item["telescope"], fire_at=fire_at,
            ))
        logger.info(f"Reloaded {self.deferred_scheduler.pending_count()} pending deferred ToO(s)")

    # ------------------------------------------------------------------
    # Auto-ToO status slot (one editable Slack message per event thread)
    # ------------------------------------------------------------------

    def _register_too_slot(self, thread_ts: str, slot: "ToOSlot") -> None:
        with self._too_slots_lock:
            self._too_slots[thread_ts] = slot
            while len(self._too_slots) > _MAX_TOO_SLOTS:
                self._too_slots.pop(next(iter(self._too_slots)))

    def _too_slot_for_thread(self, thread_ts: Optional[str]) -> "ToOSlot":
        """The event thread's slot — cached, else restored from the store.

        Keeping a single object per thread stops concurrent writers (immediate
        send callbacks, the scheduler thread, later updates) from overwriting
        each other's lines.
        """
        if not thread_ts:
            slot = ToOSlot(self.slack, self.too_slot_store)
            slot.attach(None)
            return slot
        with self._too_slots_lock:
            slot = self._too_slots.get(thread_ts)
        if slot is None:
            slot = ToOSlot(self.slack, self.too_slot_store)
            self._register_too_slot(thread_ts, slot)
            slot.attach(thread_ts)
        return slot

    @staticmethod
    def _scheduled_line(fire_at: datetime, rescheduled: bool) -> str:
        when = fire_at.astimezone(timezone.utc).strftime("%H:%M")
        if rescheduled:
            return (
                f":hourglass_flowing_sand: rescheduled for {when} UTC "
                f"(target rises later than estimated)"
            )
        return (
            f":hourglass_flowing_sand: scheduled for {when} UTC "
            f"(target rises; re-checked before send)"
        )

    def _post_deferred_result(
        self, thread_ts: Optional[str], telescope: str, sent: Optional[bool],
        reason: str = "retracted / no longer qualifies", rapid: bool = False,
        timing: str = "",
    ) -> None:
        """Append the outcome of a fired deferred ToO below its ``scheduled`` line.

        ``sent`` is ``True`` (sent), ``False`` (send failed), or ``None``
        (skipped at fire time — see ``reason``).
        """
        if sent is True:
            now = datetime.now(timezone.utc).strftime("%H:%M")
            text = (f":white_check_mark: {timing} (deferred)" if timing
                    else f":white_check_mark: sent {now} UTC (deferred)") + f" · {self._rapid_tag(rapid)}"
        elif sent is False:
            text = ":warning: failed to send — check logs, submit manually"
        else:
            text = f":no_entry_sign: not sent — {reason}"
        self._too_slot_for_thread(thread_ts).add_line(telescope, text)

    def _make_too_notice(
        self, too_slot: "ToOSlot", telescope: str, success: bool,
        notice_data: Optional[dict] = None, rapid: bool = False,
        notice_at: Optional[float] = None,
    ) -> Callable[[], None]:
        """
        Build a zero-arg callback (run on the emailer's worker thread) that
        turns the telescope's ``sending…`` line into ✅ sent / ⚠️ failed.
        ``notice_at`` (bot receipt of the notice) starts the timing shown.
        """
        def _notify() -> None:
            if success:
                sent = time.time()
                too_slot.finish_send(telescope, (
                    f":white_check_mark: {self._timing(notice_at, sent, 'notice')} "
                    f"· {self._rapid_tag(rapid)}"))
                nd = notice_data or {}
                self._track_sent(telescope, nd.get("Facility", ""), nd.get("Trigger_num", ""),
                                 "auto", nd.get("Name", ""), rapid=rapid, sent_at=sent)
            else:
                too_slot.finish_send(
                    telescope, ":warning: failed to send — check logs, submit manually"
                )

        return _notify

    # ------------------------------------------------------------------
    # ToO observation tracking (see too/obs_tracker.py)
    # ------------------------------------------------------------------

    @staticmethod
    def _rapid_tag(rapid: bool) -> str:
        return "rapid" if rapid else "normal"

    @staticmethod
    def _timing(start: Optional[float], sent: float, what: str) -> str:
        """``notice 16:51:55.12 → email sent 16:51:58.06 KST (+2.94 s)`` (KST, 0.01 s)."""
        def hms(t: float) -> str:
            dt = datetime.fromtimestamp(t, _KST)
            return f"{dt.strftime('%H:%M:%S')}.{dt.microsecond // 10000:02d}"
        if start is None:
            return f"email sent {hms(sent)} KST"
        return f"{what} {hms(start)} → email sent {hms(sent)} KST (+{sent - start:.2f} s)"

    def _track_sent(self, telescope: str, facility: Any, trigger: Any, source: str,
                    name: str = "", message: Optional[dict] = None,
                    rapid: Optional[bool] = None, requester: str = "",
                    sent_at: Optional[float] = None) -> None:
        """Tell the tracker to expect the telescope's announcement of this request."""
        if self.obs_tracker is None:
            return
        try:
            self.obs_tracker.record_sent(telescope, str(facility or ""), str(trigger or ""),
                                         source, name=name, message=message, rapid=rapid,
                                         requester=requester, sent_at=sent_at)
        except Exception as exc:
            logger.error(f"ToO tracking record failed ({telescope}): {exc}")

    def _on_manual_too_sent(
        self, facility: str, trigger: str, telescope: str, message: Optional[dict] = None,
        rapid: Optional[bool] = None, requester: str = "", sent_at: Optional[float] = None,
    ) -> None:
        """Slack-button ToO sent: dup-guard marker + tracking.

        With the tracker running, the request's status (waiting → accepted →
        observed …) is shown under its own "ToO Request Submitted" confirmation
        (``message``); otherwise a line in the event's ToO slot records the send.
        """
        self.notice_handler.mark_too_sent(facility, trigger, telescope)
        entry = self.notice_handler.lookup_thread(facility, trigger) or {}
        tracker = self.obs_tracker
        if message and tracker is not None and tracker.running and tracker.tracks(telescope):
            self._track_sent(telescope, facility, trigger, "manual", entry.get("name", ""),
                             message=message, rapid=rapid, requester=requester, sent_at=sent_at)
            self._update_request_status(telescope, {**message, "lines": []})
            return
        if entry.get("thread_ts"):
            stamp = datetime.fromtimestamp(sent_at or time.time(), _KST).strftime("%H:%M:%S")
            self._too_slot_for_thread(entry["thread_ts"]).add_line(
                telescope, f":raised_hand: manual request sent {stamp} KST"
                + (f" · {self._rapid_tag(rapid)}" if rapid is not None else "")
            )
        # name: matches the request even if its Comments were edited in the form
        self._track_sent(telescope, facility, trigger, "manual", entry.get("name", ""),
                         rapid=rapid, requester=requester, sent_at=sent_at)

    def _tracker_window(self, ra: float, dec: float) -> Optional[tuple]:
        """Tracker callback (tracker thread): (start, end) epochs of the target's
        current / next observable window at CTIO tonight, or ``None``.

        Uses its own VisibilityManager — the notice pipeline's is used on the
        main thread at the same time.
        """
        if getattr(self, "_tracker_vis", None) is None:
            from visibility.plotter import VisibilityManager
            self._tracker_vis = VisibilityManager(
                min_altitude=self.config.min_altitude, min_moon_sep=self.config.min_moon_sep,
            )
        status = self._tracker_vis.get_status(ra=ra, dec=dec)
        if not status or status.get("case") not in ("observable_now", "observable_later"):
            return None
        window = ((status.get("details") or {}).get("tonight") or {}).get("window") or {}

        def epoch(key: str) -> Optional[float]:
            raw = str(window.get(key) or "")
            try:
                return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc).timestamp()
            except ValueError:
                return None

        start, end = epoch("start_time_utc"), epoch("end_time_utc")
        if status["case"] == "observable_now" and start is None:
            start = time.time()
        return (start, end) if start is not None and end is not None else None

    def _update_request_status(self, telescope: str, target: dict) -> bool:
        """Re-render a request's own message: its text + status lines (or 'waiting')."""
        lines = target.get("lines") or []
        if not lines:
            channel = self.obs_tracker.channels.get(telescope, "") if self.obs_tracker else ""
            lines = [f":hourglass_flowing_sand: waiting for {telescope} to accept the request"
                     + (f" (<#{channel}>)" if channel else "")]
        try:
            self.slack.client.chat_update(
                channel=target["channel"], ts=target["ts"],
                text=target["text"] + "\n" + "\n".join(lines),
            )
            return True
        except Exception as exc:
            logger.warning(f"ToO request status edit failed ({target.get('ts')}): {exc}")
            return False

    def _on_obs_line(
        self, telescope: str, facility: str, trigger: str, text: str,
        target: Optional[dict] = None,
    ) -> None:
        """Tracker callback (tracker thread): the request's own message when it has
        one (manual confirmation), else the event's ToO slot."""
        if target and target.get("ts") and self._update_request_status(telescope, target):
            return
        thread_ts = (self.notice_handler.lookup_thread(facility, trigger) or {}).get("thread_ts")
        if not thread_ts:
            logger.info(f"ToO tracking: no thread for {facility} #{trigger} — {text}")
            return
        self._too_slot_for_thread(thread_ts).add_line(telescope, text)

    def _upload_visibility_plot(
        self,
        thread_ts: str,
        ra: float,
        dec: float,
        title: str = "Visibility Plot",
        observatory: str = "7DT",
    ) -> None:
        """Upload a visibility plot for *observatory* (``"7DT"`` or ``"LOAO"``) to a thread."""
        if observatory == "LOAO":
            if not self.visibility.available_loao:
                return
            generate = lambda: self.visibility.generate_plot_loao(ra=ra, dec=dec)
        else:
            if not self.visibility.available:
                return
            generate = lambda: self.visibility.generate_plot(ra=ra, dec=dec)

        try:
            buf = generate()
            if buf:
                self.slack.upload_file(
                    thread_ts=thread_ts,
                    file=buf,
                    filename=f"visibility_{observatory.lower()}.png",
                    title=title,
                )
        except Exception as exc:
            logger.error(f"Failed to upload {observatory} visibility plot: {exc}")

    # ------------------------------------------------------------------
    # Same-burst links (one burst seen by several facility families)
    # ------------------------------------------------------------------

    @staticmethod
    def _dispatched_tiles(
        dispatched: List[str], tile_result: Optional[dict], tile_result_rasa36: Optional[dict],
    ) -> dict:
        """Tile IDs each dispatched auto-ToO requested (stored as ToO_Tiles_*)."""
        results = {"7DT": tile_result, "RASA36": tile_result_rasa36}
        return {t: [str(i) for i in ((results.get(t) or {}).get("tile_ids") or [])] for t in dispatched}

    def _sigma(self, err: Any, facility: str) -> float:
        return linking.sigma(linking.to_float(err), linking.family_of(facility), self.link_params)

    def _group_siblings(self, notice_data: dict, link: Optional[dict]) -> List[dict]:
        """The other members of the event's same-burst group (for a brand-new
        event: of the group *link* says it is joining)."""
        nh = self.notice_handler
        key = nh._thread_key(notice_data.get("Facility", ""), notice_data.get("Trigger_num", ""))
        return [m for m in nh.group_members(link["root"] if link else key) if m["key"] != key]

    def _best_in_group(self, notice_data: dict, root_key: str) -> "tuple[bool, Optional[dict]]":
        """(is *notice_data* the burst's best localization so far?, best existing member)."""
        mine = self._sigma(notice_data.get("Error"), notice_data.get("Facility", ""))
        members = [m for m in self.notice_handler.group_members(root_key) if m.get("err") is not None]
        best = min(members, key=lambda m: self._sigma(m["err"], m["family"]), default=None)
        if best is None:
            return True, None
        return mine <= self._sigma(best["err"], best["family"]), best

    def _group_too_block(
        self, telescope: str, notice_data: dict, tile_result: Optional[dict], link: Optional[dict],
    ) -> "Optional[tuple[str, bool]]":
        """Why a linked event must not auto-ToO on *telescope* — ``(reason,
        show_in_slot)`` — or ``None`` when it may.

        1. A linked event has a smaller localization sigma: the burst is where
           that one says, so this coarser one never sends (not shown — the
           better event decides for itself, in its own thread).
        2. A linked event already sent a ToO for exactly these tiles: a second
           request would only duplicate it (shown). A different tile set — a
           real refinement — still sends.
        """
        try:
            siblings = self._group_siblings(notice_data, link)
        except Exception as exc:
            logger.error(f"Same-burst ToO guard lookup failed: {exc}")
            return None
        if not siblings:
            return None
        mine = self._sigma(notice_data.get("Error"), notice_data.get("Facility", ""))
        better = [m for m in siblings
                  if m.get("err") is not None and self._sigma(m["err"], m["family"]) < mine]
        if better:
            b = min(better, key=lambda m: self._sigma(m["err"], m["family"]))
            return (f"superseded — {b['name']} ({b['facility']} #{b['trigger']}) has a better "
                    f"localization (err {self.formatter.fmt_err(b['err'])})", False)
        my_tiles = sorted(str(t) for t in ((tile_result or {}).get("tile_ids") or []))
        if not my_tiles:
            return None
        for m in siblings:
            sent_at = m["too"].get(telescope)
            if not sent_at:
                continue
            prev = sorted(m["too_tiles"].get(telescope) or []) or self._member_tiles(telescope, m)
            if prev == my_tiles:
                when = sent_at.replace("T", " ").replace("Z", " UTC")
                return (f"{m['name']} ({m['facility']} #{m['trigger']}) already requested the same "
                        f"tile{'s' if len(my_tiles) > 1 else ''} ({', '.join(my_tiles)}) at {when}", True)
        return None

    def _member_tiles(self, telescope: str, member: dict) -> List[str]:
        """Tiles at a linked event's position — used when its ToO stored none (manual send)."""
        _, tiles = self._telescope_ctx(telescope)
        if tiles is None or member.get("ra") is None or member.get("dec") is None or not member.get("err"):
            return []
        try:
            info = tiles.get_tile_info(ra=member["ra"], dec=member["dec"], error=float(member["err"]))
        except Exception as exc:
            logger.warning(f"Tile lookup for linked event {member['key']} failed: {exc}")
            return []
        return sorted(str(t) for t in ((info or {}).get("tile_ids") or []))

    def _post_same_burst_link(
        self, notice_data: dict, new_thread_ts: str, link: dict, best: bool,
        best_member: Optional[dict], canon_url: str,
        tile_result: Optional[dict], tile_result_rasa36: Optional[dict],
        visibility_result: Optional[dict],
    ) -> None:
        """Record a notice-time same-burst link and cross-link both threads."""
        nh = self.notice_handler
        own_key = nh._thread_key(notice_data.get("Facility", ""), notice_data.get("Trigger_num", ""))
        if nh.lookup_thread(link["matched_facility"], link["matched_trigger"]) is None:
            nh.record_thread(link["matched_facility"], link["matched_trigger"],
                             link["matched_thread_ts"], link["matched_name"])
        evidence = {k: (round(link[k], 3) if isinstance(link[k], float) else link[k])
                    for k in ("rule", "dt_s", "sep_deg", "tol_deg")}
        root = nh.link_events(own_key, link["key"], evidence)
        if root is None:
            logger.warning(f"Same-burst link {own_key} → {link['key']} could not be recorded")
            return
        if not self.send_to_slack:
            return
        try:
            self.slack.send_thread_message(
                thread_ts=new_thread_ts,
                text=self.formatter.same_burst_new_thread(link, canon_url, best, best_member),
            )
        except Exception as exc:
            logger.error(f"Failed to post same-burst note to new thread: {exc}")
        try:
            new_url = self.slack.get_permalink(new_thread_ts)
            self.slack.send_thread_message(
                thread_ts=link["thread_ts"],
                text=self.formatter.same_burst_quote(
                    notice_data, link, new_url, best, tile_result, tile_result_rasa36, visibility_result,
                ),
            )
        except Exception as exc:
            logger.error(f"Failed to post same-burst note to canonical thread: {exc}")
        logger.info(f"Same-burst threads cross-linked: {own_key} ({new_thread_ts}) ↔ {root} ({link['thread_ts']})")

    def _too_link_note(self, facility: str, trigger: str, telescope: str) -> str:
        """Warning lines for the manual ToO modal of a linked event ("" if none)."""
        nh = self.notice_handler
        key = nh._thread_key(facility, trigger)
        members = nh.group_members(key)
        me = next((m for m in members if m["key"] == key), None)
        others = [m for m in members if m["key"] != key]
        if me is None or not others:
            return ""
        lines = []
        for m in others:
            if m["too"].get(telescope):
                lines.append(f":link: A {telescope} ToO was already sent for this burst via linked "
                             f"*{m['name']}* ({m['facility']} #{m['trigger']}) at "
                             f"{m['too'][telescope].replace('T', ' ')}.")
        mine = self._sigma(me.get("err"), me["family"])
        better = [m for m in others if m.get("err") is not None and self._sigma(m["err"], m["family"]) < mine]
        if better:
            b = min(better, key=lambda m: self._sigma(m["err"], m["family"]))
            lines.append(f":dart: Linked {b['facility']} #{b['trigger']} has a better localization "
                         f"(err {self.formatter.fmt_err(b['err'])} vs {self.formatter.fmt_err(me.get('err'))}) "
                         "— the button in its thread uses that position.")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Connection callbacks (passed to GCNConsumer.start_connection_monitor)
    # ------------------------------------------------------------------

    def _on_connection_lost(self) -> None:
        elapsed = self.consumer._elapsed_since_heartbeat()
        last_hb = self.consumer._last_heartbeat_time()
        raw     = self.formatter.format_connection_lost(last_hb, elapsed)
        blocks  = raw.get("blocks", []) if isinstance(raw, dict) else raw
        if self.send_to_slack:
            self.slack.send_message(blocks=blocks, text="⚠️ GCN connection lost")

    def _on_connection_restored(self) -> None:
        last_hb = self.consumer._last_heartbeat_time()
        raw     = self.formatter.format_connection_restored(last_hb)
        blocks  = raw.get("blocks", []) if isinstance(raw, dict) else raw
        if self.send_to_slack:
            self.slack.send_message(blocks=blocks, text="✅ GCN connection restored")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Start all background services and enter the Kafka polling loop.
        Blocks until interrupted or the consumer signals it should stop.
        """
        # Verify Slack connectivity
        try:
            resp = self.slack.client.api_test()
            if not resp["ok"]:
                logger.error(f"Slack auth failed: {resp.get('error')} — continuing anyway")
            else:
                logger.info("Slack authentication OK")
        except Exception as exc:
            logger.error(f"Slack auth check error: {exc} — continuing anyway")

        # Register Bolt action / view handlers (7DT + RASA36)
        if self.slack.app:
            try:
                self.too_handler.register_handlers(self.slack.app)
                self.too_handler_rasa36.register_handlers(self.slack.app)

                # "📄 GCN Notice" is a URL button: Slack opens the link itself, but
                # still sends a block_actions event that Bolt would log as an
                # unhandled request (WARNING + 404). Acknowledge it and do nothing.
                @self.slack.app.action("notice_link")
                def _ack_notice_link(ack):
                    ack()

                logger.info("Slack Bolt handlers registered (7DT + RASA36)")
            except Exception as exc:
                logger.error(f"Failed to register Slack handlers: {exc}")

        # Start Socket Mode (interactive features)
        self.slack.start_socket_mode()

        # Start connection monitor in background
        self.consumer.start_connection_monitor(
            on_lost=self._on_connection_lost,
            on_restored=self._on_connection_restored,
        )

        # Start the deferred-ToO scheduler and re-arm any ToOs left pending in
        # the ASCII file (e.g. the bot restarted while a target was still rising).
        self.deferred_scheduler.start()

        try:
            self._reload_deferred_too()
        except Exception as exc:
            logger.error(f"Deferred ToO reload failed: {exc}", exc_info=True)

        # Follow sent ToOs through the telescopes' alert channels (edits Slack,
        # so only when posting is on).
        if self.obs_tracker is not None and self.send_to_slack:
            self.obs_tracker.start()

        # Handle OS signals — just flip the flag; shutdown() runs from the finally block
        signal.signal(signal.SIGTERM, lambda *_: setattr(self.consumer, "_running", False))
        signal.signal(signal.SIGINT,  lambda *_: setattr(self.consumer, "_running", False))

        logger.info("Starting GCN Kafka polling loop — press Ctrl-C to stop")
        try:
            self.consumer.start(on_message=self.process_notice)
        except SystemExit:
            pass
        except Exception as exc:
            logger.error(f"Consumer exited with error: {exc}", exc_info=True)
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Gracefully stop all components."""
        if getattr(self, "_shutdown_called", False):
            return
        self._shutdown_called = True
        logger.info("Shutting down GCNBot…")
        try:
            self.deferred_scheduler.stop()
        except Exception as exc:
            logger.warning(f"Deferred scheduler stop error: {exc}")
        if self.obs_tracker is not None:
            try:
                self.obs_tracker.stop()
            except Exception as exc:
                logger.warning(f"ToO observation tracker stop error: {exc}")
        try:
            self.consumer.stop()
        except Exception as exc:
            logger.warning(f"Consumer stop error: {exc}")
        for em in (self.emailer, self.emailer_rasa36):
            try:
                em.close_smtp()
            except Exception as exc:
                logger.warning(f"SMTP close error: {exc}")
        try:
            self.slack.stop_socket_mode()
        except Exception as exc:
            logger.warning(f"Socket Mode stop error: {exc}")
        logger.info("GCNBot shutdown complete")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="GCN Alert Bot — monitors GCN Kafka topics and posts to Slack"
    )
    parser.add_argument(
        "--include-test",
        action="store_true",
        default=False,
        help="Process topics that contain '_TEST' in their name",
    )
    parser.add_argument(
        "--send",
        action="store_true",
        default=False,
        help="Force Slack message delivery (useful with --include-test)",
    )
    parser.add_argument(
        "--config",
        metavar="PATH",
        default="",
        help="Path to an alternative settings.toml",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log verbosity (default: INFO)",
    )
    parser.add_argument(
        "--log-file",
        metavar="PATH",
        default="",
        help="Write log output to this file in addition to stdout",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))

    _log_banner("GCN Alert Bot — starting up")

    from config import BotConfig

    config = BotConfig(config_path=args.config)

    # Attach a FileHandler if a log file is specified (CLI takes precedence over config).
    log_file = args.log_file or config.log_file
    if log_file:
        _fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        _fh = logging.FileHandler(log_file, encoding="utf-8")
        _fh.setFormatter(_fmt)
        logging.getLogger().addHandler(_fh)
        logger.info(f"Logging to file: {log_file}")

    # In production (no --include-test), always send.
    # With --include-test the user controls whether to send via --send.
    send_to_slack = True if not args.include_test else args.send

    bot = GCNBot(
        config=config,
        send_to_slack=send_to_slack,
        include_test_topics=args.include_test,
    )
    bot.run()


if __name__ == "__main__":
    main()
