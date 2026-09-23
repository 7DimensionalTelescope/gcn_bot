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
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, List, Optional

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


class _DeferredThreadTs:
    """
    Holds a Slack ``thread_ts`` that is produced only after the main event
    message is posted, but is needed earlier by the async auto-ToO callbacks.

    The auto-ToO email is dispatched *before* the Slack post (so it runs in
    parallel with plot rendering/uploads). Its success/failure notice must be
    threaded under the main message, so the worker thread calls :meth:`get`,
    which blocks briefly until :meth:`set` is called from the notice handler.
    On timeout (Slack post slow or failed) ``get`` returns ``None`` and the
    caller falls back to a channel post.
    """

    def __init__(self) -> None:
        self._event = threading.Event()
        self._ts: Optional[str] = None

    def set(self, ts: Optional[str]) -> None:
        """Publish the thread_ts (idempotent — first call wins)."""
        if not self._event.is_set():
            self._ts = ts or None
            self._event.set()

    def get(self, timeout: float = 5.0) -> Optional[str]:
        """Block up to *timeout* seconds for the thread_ts; ``None`` if unset."""
        self._event.wait(timeout)
        return self._ts


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

        # --- Notice / Circular storage ---
        self.notice_handler = GCNNoticeHandler(
            output_csv=config.output_notice_csv,
            output_ascii=config.output_ascii,
            ascii_max_events=config.ascii_max_events,
        )
        self.circular_handler = GCNCircularHandler(
            output_csv=config.output_circular_csv,
            output_ascii=config.output_ascii,
            ascii_max_events=config.ascii_max_events,
        )

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
        )
        self.emailer_rasa36 = GCNToOEmailer(
            email_from=config.email_from,
            email_to=config.email_to_rasa36,
            email_password=config.email_password,
            telescope="RASA36",
            auto_too_grb_only=config.too_auto_grb_only,
            auto_too_max_tiles=config.auto_too_max_tiles,
            observable_soon_hours=config.too_observable_soon_hours,
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

        # --- Slack ToO handlers (one per telescope) ---
        self.too_handler = SlackToOHandler(
            slack=self.slack,
            emailer=self.emailer,
            user_group=config.too_user_group,
            too_config=config.too_config,
            telescope="7DT",
            on_sent=self.notice_handler.mark_too_sent,
        )
        self.too_handler_rasa36 = SlackToOHandler(
            slack=self.slack,
            emailer=self.emailer_rasa36,
            user_group=config.too_user_group,
            too_config=config.too_config,
            telescope="RASA36",
            on_sent=self.notice_handler.mark_too_sent,
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

    def _handle_circular_slack(self, processed: dict) -> None:
        """Post a message to the matching Slack thread for a processed circular.

        Decision tree
        -------------
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

        # ---- Find the matching ASCII event (two passes) ----
        ascii_event: Optional[dict] = None
        name_corrected: Optional[tuple] = None  # (old_name, new_name) if a rename occurred

        # Pass 1 — match by official event_id in the Name column (space-insensitive)
        if event_id:
            try:
                ascii_event = self.notice_handler.find_event_by_name(event_id)
            except Exception as exc:
                logger.error(f"find_event_by_name failed: {exc}")

        # Pass 2 — match by facility + trigger_num
        if ascii_event is None and facility and trigger_num:
            try:
                ascii_event = self.notice_handler.find_existing_event(
                    facility, trigger_num, return_full_data=True
                )
                # When GCN reuses an event name after a false-trigger retraction,
                # our ASCII may have a provisional name (e.g. "EP 260527b") while
                # the official eventId is "EP260527a".  Update the ASCII Name now
                # and remember the correction for the Slack notification below.
                if ascii_event is not None and event_id:
                    stored_name = str(ascii_event.get("Name", "")).strip().strip('"')
                    if stored_name.lower().replace(" ", "") != event_id.lower().replace(" ", ""):
                        try:
                            self.notice_handler.update_ascii_event_name(stored_name, event_id)
                            ascii_event = dict(ascii_event)
                            ascii_event["Name"] = event_id
                            name_corrected = (stored_name, event_id)
                            logger.info(
                                f"Updated ASCII Name '{stored_name}' → '{event_id}' "
                                f"(trigger match, circular {circular_id})"
                            )
                        except Exception as exc:
                            logger.error(f"update_ascii_event_name failed: {exc}")
            except Exception as exc:
                logger.error(f"find_existing_event failed: {exc}")

        if ascii_event is None:
            logger.info(
                f"No ASCII match for circular {circular_id} "
                f"(event_id={event_id!r}, facility={facility!r}, trigger={trigger_num!r}) "
                "— skipping Slack post"
            )
            return

        thread_ts = str(ascii_event.get("thread_ts", "")).strip().strip('"')
        if not thread_ts:
            logger.info(f"Circular {circular_id}: ASCII row has no thread_ts — skipping Slack post")
            return

        # ---- Post rename notification first (if name was corrected via trigger match) ----
        if name_corrected and self.send_to_slack:
            old_n, new_n = name_corrected
            self.slack.send_thread_message(
                thread_ts=thread_ts,
                text=f":pencil: Event renamed: *{old_n}* → *{new_n}* (official GCN event ID)",
            )

        # ---- Build and send the circular thread message ----
        circular_url = f"https://gcn.nasa.gov/circulars/{circular_id}"
        link = f"<{circular_url}|{subject}>"

        if is_false:
            text = f":warning: *False trigger* — {link}"
        elif is_first and event_page:
            # Update ASCII Name to the official event_id if it was provisional
            # (only needed if Pass 1 matched with a space difference; Pass 2 renames
            # are already handled above by name_corrected).
            if event_id:
                old_name = str(ascii_event.get("Name", "")).strip().strip('"')
                if old_name.lower().replace(" ", "") != event_id.lower().replace(" ", ""):
                    try:
                        self.notice_handler.update_ascii_event_name(old_name, event_id)
                    except Exception as exc:
                        logger.error(f"update_ascii_event_name failed: {exc}")
            text = f":bell: First circular: {link}\nEvent page: <{event_page}|{event_page}>"
        else:
            text = f":memo: {link}"

        resp = self.slack.send_thread_message(thread_ts=thread_ts, text=text)
        if resp:
            logger.info(f"Circular {circular_id} posted to thread {thread_ts}")
        else:
            logger.error(f"Failed to post circular {circular_id} to Slack thread {thread_ts}")

    # ------------------------------------------------------------------
    # Internal: notice pipeline
    # ------------------------------------------------------------------

    def _process_notice_message(self, topic: str, value: Any) -> None:
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
                    is_update = bool(existing_thread_ts)
                    logger.info(
                        f"Existing event found for {facility} #{trigger_num} "
                        f"(thread_ts={existing_thread_ts})"
                    )
            except Exception as exc:
                logger.error(f"Error looking up existing event: {exc}")

        # 3. Visibility — 7DT (Chile) and LOAO (Arizona)
        visibility_result      = None
        visibility_result_loao = None
        if ra is not None and dec is not None:
            try:
                visibility_result = self.visibility.get_status(ra=ra, dec=dec)
                if visibility_result:
                    logger.info(
                        f"7DT visibility for {name}: {visibility_result.get('case')}"
                    )
            except Exception as exc:
                logger.error(f"7DT visibility analysis failed: {exc}")
            try:
                visibility_result_loao = self.visibility.get_status_loao(ra=ra, dec=dec)
                if visibility_result_loao:
                    logger.info(
                        f"LOAO visibility for {name}: {visibility_result_loao.get('case')}"
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
                        logger.info(f"7DT tile coverage for {name}: {tile_result['n_tiles']} tile(s)")
                    tile_result_rasa36 = self.tiles_rasa36.get_tile_info(ra=ra, dec=dec, error=float(error))
                    if tile_result_rasa36:
                        logger.info(f"RASA36 tile coverage for {name}: {tile_result_rasa36['n_tiles']} tile(s)")
            except (ValueError, TypeError, Exception) as exc:
                logger.error(f"Tile coverage analysis failed: {exc}")

        # 4. Storage
        csv_status   = False
        ascii_status = False

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
        # deferred holder for immediate threading.
        too_dispatched: List[str] = []
        if self.config.turn_on_too_email_auto_on_update:
            too_thread = _DeferredThreadTs()
            too_thread.set(existing_thread_ts)
            too_dispatched = self._dispatch_auto_too(
                notice_data, visibility_result, tile_result, tile_result_rasa36, too_thread
            )

        # Save ASCII with the existing thread_ts (and any ToO-sent marker) so the
        # row is refreshed.
        try:
            self.notice_handler.save_to_ascii(
                notice_data, existing_thread_ts, too_sent=too_dispatched
            )
        except Exception as exc:
            logger.error(f"ASCII save (update) failed: {exc}")

        # Compare to find meaningful changes
        differences = self.notice_handler.compare_event_data(existing_event, notice_data)
        if not differences:
            logger.info("No meaningful differences found — skipping thread update")
            return

        coord_changed = isinstance(differences, dict) and "coordinates" in differences

        # Build tile suffix for the update text
        tile_suffix = ""
        if tile_result is not None:
            n = tile_result["n_tiles"]
            tile_suffix = f"\n - 7DT Tiles: {n} tile{'s' if n != 1 else ''}"
        if tile_result_rasa36 is not None:
            n = tile_result_rasa36["n_tiles"]
            tile_suffix += f"\n - RASA36 Tiles: {n} tile{'s' if n != 1 else ''}"

        thread_update = self.formatter.format_thread_update(differences, notice_data)
        if isinstance(thread_update, list):
            send_kwargs = dict(thread_ts=existing_thread_ts, blocks=thread_update, text="GCN Notice Update")
        else:
            send_kwargs = dict(thread_ts=existing_thread_ts, text=str(thread_update) + tile_suffix)

        if self.send_to_slack:
            # Upload tile plots first (so they appear above the text in the thread)
            if coord_changed and ra is not None and dec is not None:
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
            if coord_changed and ra is not None and dec is not None:
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
    ) -> None:
        """Post the initial Slack message for a brand-new event."""
        # Dispatch the auto-ToO email FIRST, before any Slack work. The send is
        # async, so the email now fires in parallel with plot rendering/uploads
        # (which follow) instead of waiting ~5-15s behind them. Its inputs
        # (visibility_result + tile_result) are already computed. The success/
        # failure notice threads under the main message via `too_thread`, whose
        # thread_ts is filled in once the Slack post below returns.
        too_thread = _DeferredThreadTs()
        too_dispatched = self._dispatch_auto_too(
            notice_data, visibility_result, tile_result, tile_result_rasa36, too_thread
        )

        # Save to ASCII first (without thread_ts) to get storage status. The
        # ToO-sent marker is folded in here — this write always runs (unlike the
        # thread_ts update below, which is skipped when the Slack post fails), so
        # the guard survives a Slack-failure re-entry of this path.
        ascii_status = False
        try:
            ascii_status = self.notice_handler.save_to_ascii(
                notice_data, too_sent=too_dispatched
            )
        except Exception as exc:
            logger.error(f"ASCII save (new event) failed: {exc}")

        # Format main message.
        # format_notice returns (dict_or_None, lc_url, notice_url); extract blocks list.
        raw_result, lc_url, notice_url = self.formatter.format_notice(
            topic=topic,
            value=value,
            csv_status=csv_status,
            ascii_status=ascii_status,
            notice_data=notice_data,
            test_mode=self.include_test_topics,
        )
        if raw_result is None:
            logger.error("Message formatter returned None — aborting")
            too_thread.set(None)  # unblock any waiting ToO-notice callbacks
            return
        blocks: list = raw_result.get("blocks", []) if isinstance(raw_result, dict) else list(raw_result)
        if not blocks:
            logger.error("Message formatter returned empty blocks — aborting")
            too_thread.set(None)  # unblock any waiting ToO-notice callbacks
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
            too_thread.set(None)  # no auto-ToO dispatched in this mode, but stay safe
            return

        # Send main message
        resp = self.slack.send_message(
            blocks=blocks,
            text=f"GCN Alert: {notice_data.get('Name', 'New Target')}",
        )
        if not resp:
            logger.error("Failed to send Slack message")
            too_thread.set(None)  # unblock ToO-notice callbacks → channel fallback
            return

        new_thread_ts = resp.get("ts", "")
        # Publish thread_ts so the already-dispatched auto-ToO notices thread here.
        too_thread.set(new_thread_ts)
        logger.info(
            f"New message posted (thread_ts={new_thread_ts}) for "
            f"{notice_data.get('Facility')} #{notice_data.get('Trigger_num')}"
        )

        # Update ASCII row with real thread_ts
        try:
            self.notice_handler.save_to_ascii(notice_data, new_thread_ts)
        except Exception as exc:
            logger.error(f"Failed to update ASCII with thread_ts: {exc}")

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

        # Cross-match: notify both threads when a related event is found
        self._post_crossmatch_notifications(notice_data, new_thread_ts)

    # ------------------------------------------------------------------
    # Internal: helpers
    # ------------------------------------------------------------------

    def _dispatch_auto_too(
        self,
        notice_data: dict,
        visibility_result: Optional[dict],
        tile_result: Optional[dict],
        tile_result_rasa36: Optional[dict],
        too_thread: "_DeferredThreadTs",
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

        Gated on ``send_to_slack`` to preserve prior behaviour: in dev mode
        without ``--send``, no automatic ToO emails go out. Each telescope×case
        combination has its own enable flag (checked inside
        ``_dispatch_auto_too_one`` once the tile case is known). Success/failure
        notices thread under the main message once ``too_thread`` is set.
        """
        dispatched: List[str] = []
        if not self.send_to_slack:
            return dispatched
        if self._dispatch_auto_too_one(
            "7DT", self.emailer, tile_result,
            notice_data, visibility_result, too_thread
        ):
            dispatched.append("7DT")
        if self._dispatch_auto_too_one(
            "RASA36", self.emailer_rasa36, tile_result_rasa36,
            notice_data, visibility_result, too_thread
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
        too_thread: "_DeferredThreadTs",
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
        target_name = notice_data.get("Name", "Unknown")
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

        if case == "now":
            # A now-send supersedes any pending deferred entry for this event.
            if self.deferred_scheduler.is_pending(key):
                self.deferred_scheduler.cancel(key)
                self.notice_handler.clear_too_deferred(facility, trigger, telescope)
            too_config = self._auto_too_config(telescope, n_tiles)
            try:
                emailer.send_too_email_async(
                    notice_data,
                    too_config=too_config,
                    on_success=self._make_too_notice(too_thread, telescope, target_name, True),
                    on_failure=self._make_too_notice(too_thread, telescope, target_name, False),
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
            self._notify_too_scheduled(too_thread, telescope, target_name, fire_at)
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
            self._post_deferred_result(thread_ts, telescope, name, sent=None)
            return

        if case == "now":
            too_config = self._auto_too_config(telescope, n_tiles)
            ok = emailer.send_too_email(notice_data, too_config)
            if ok:
                self.notice_handler.mark_too_sent(facility, trigger, telescope)
            # Clear the pending marker either way; a failed send is not retried
            # (the operator is notified to submit manually).
            self.notice_handler.clear_too_deferred(facility, trigger, telescope)
            self._post_deferred_result(thread_ts, telescope, name, sent=ok)
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
            logger.info(
                f"Deferred ToO {entry.key}: target still rising — rescheduled to "
                f"{new_fire.isoformat()}"
            )
        else:
            logger.info(
                f"Deferred ToO {entry.key}: no longer qualifies (case={case}) — skipping"
            )
            self.notice_handler.clear_too_deferred(facility, trigger, telescope)
            self._post_deferred_result(thread_ts, telescope, name, sent=None)

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

    def _notify_too_scheduled(
        self, too_thread: "_DeferredThreadTs", telescope: str,
        target_name: str, fire_at: datetime,
    ) -> None:
        """Post a '⏳ ToO scheduled' notice under the event thread (non-blocking).

        Runs on a short-lived daemon thread so it can wait for ``too_thread``
        (the main message may still be posting) without blocking the caller.
        """
        when = fire_at.strftime("%Y-%m-%d %H:%M UTC")
        text = (
            f":hourglass_flowing_sand: Automatic {telescope} ToO *scheduled* for "
            f"*{target_name}* at {when} (when the target rises; a final "
            f"retraction / visibility / tile check runs before it sends)."
        )

        def _task() -> None:
            thread_ts = too_thread.get(timeout=5.0)
            if thread_ts:
                self.slack.send_thread_message(thread_ts=thread_ts, text=text)
            else:
                self.slack.send_message(blocks=[], text=text)

        threading.Thread(target=_task, name="too-scheduled-notice", daemon=True).start()

    def _post_deferred_result(
        self, thread_ts: Optional[str], telescope: str,
        target_name: str, sent: Optional[bool],
    ) -> None:
        """Post the outcome of a fired deferred ToO under the event thread.

        ``sent`` is ``True`` (sent), ``False`` (send failed), or ``None``
        (skipped at fire time — no longer qualifies / retracted).
        """
        if sent is True:
            text = (
                f":white_check_mark: Deferred {telescope} ToO email *sent* for "
                f"*{target_name}* (target now observable)."
            )
        elif sent is False:
            text = (
                f":warning: Deferred {telescope} ToO email *failed to send* for "
                f"*{target_name}*. Check the bot logs and submit manually if needed."
            )
        else:
            text = (
                f":no_entry_sign: Deferred {telescope} ToO for *{target_name}* "
                f"*not sent* — at fire time it no longer qualified (retracted, no "
                f"longer a single tile, or not observable)."
            )
        if thread_ts:
            self.slack.send_thread_message(thread_ts=thread_ts, text=text)
        else:
            self.slack.send_message(blocks=[], text=text)

    def _make_too_notice(
        self,
        too_thread: "_DeferredThreadTs",
        telescope: str,
        target_name: str,
        success: bool,
    ) -> Callable[[], None]:
        """
        Build a zero-arg callback (run on the emailer's worker thread) that
        posts an auto-ToO send result as a reply under the original event
        thread. ``success`` selects the ✅ sent / ⚠️ failed message.

        The callback waits briefly for ``too_thread`` (the main message may
        still be posting); on timeout it falls back to a channel post.
        """
        if success:
            text = (
                f":white_check_mark: Automatic {telescope} ToO email *sent* "
                f"for *{target_name}*."
            )
        else:
            text = (
                f":warning: Automatic {telescope} ToO email *failed to send* "
                f"for *{target_name}*. Check the bot logs and submit manually "
                f"if needed."
            )

        def _notify() -> None:
            thread_ts = too_thread.get(timeout=5.0)
            if thread_ts:
                self.slack.send_thread_message(thread_ts=thread_ts, text=text)
            else:
                # Main message unavailable — post to the channel so it's not lost.
                self.slack.send_message(blocks=[], text=text)

        return _notify

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

    def _post_crossmatch_notifications(
        self,
        notice_data: dict,
        new_thread_ts: str,
    ) -> None:
        """Post coincidence warnings to the new event's thread and each related event's thread."""
        try:
            related = self.notice_handler.find_related_events(
                notice_data,
                leniency_factor=self.config.crossmatch_leniency_factor,
                time_window_hours=self.config.crossmatch_time_window_hours,
            )
        except Exception as exc:
            logger.error(f"Cross-match lookup failed: {exc}")
            return

        if not related:
            return

        new_name     = notice_data.get("Name") or notice_data.get("GCN_ID", "this event")
        new_facility = notice_data.get("Facility", "")
        new_norm_fac = self.notice_handler._normalize_facility(new_facility)

        # Only quote events from a different facility; same-facility updates go through
        # the thread-update path and should never generate cross-match notifications.
        related = [
            rel for rel in related
            if self.notice_handler._normalize_facility(
                str(rel.get("Primary_Facility", "")).strip().strip('"')
            ) != new_norm_fac
        ]
        if not related:
            return

        new_url = self.slack.get_permalink(new_thread_ts) if self.send_to_slack else ""

        for rel in related:
            rel_name = (
                str(rel.get("Name", "")).strip().strip('"')
                or str(rel.get("GCN_ID", "")).strip().strip('"')
            )
            rel_fac  = str(rel.get("Primary_Facility", "")).strip().strip('"')
            rel_ts   = str(rel.get("thread_ts", "")).strip().strip('"')

            if self.send_to_slack:
                rel_url = self.slack.get_permalink(rel_ts) if rel_ts else ""
                rel_ref = f"<{rel_url}|{rel_name}>" if rel_url else f"*{rel_name}*"
                msg_new = (
                    f":telescope: This event may be spatially and temporally coincident with "
                    f"{rel_ref} ({rel_fac}). Please verify via Circular."
                )
                try:
                    self.slack.send_thread_message(thread_ts=new_thread_ts, text=msg_new)
                    logger.info(f"Cross-match note → new thread {new_thread_ts} (related={rel_name})")
                except Exception as exc:
                    logger.error(f"Failed to post cross-match note to new thread: {exc}")

                if rel_ts:
                    new_ref = f"<{new_url}|{new_name}>" if new_url else f"*{new_name}*"
                    msg_rel = (
                        f":telescope: A new notice ({new_ref}, {new_facility}) may be spatially "
                        f"and temporally coincident with this event. Please verify via Circular."
                    )
                    try:
                        self.slack.send_thread_message(thread_ts=rel_ts, text=msg_rel)
                        logger.info(f"Cross-match note → related thread {rel_ts} (new={new_name})")
                    except Exception as exc:
                        logger.error(f"Failed to post cross-match note to related thread: {exc}")

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
        try:
            self.consumer.stop()
        except Exception as exc:
            logger.warning(f"Consumer stop error: {exc}")
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
