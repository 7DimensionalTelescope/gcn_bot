"""
ToO status slot — one Slack message per event thread, edited in place.

Instead of posting a new "ToO sent" reply for every telescope/outcome (which
lands wherever the SMTP call happens to finish among the plot uploads), the
bot posts a single ``🤖 ToO status`` reply directly under the event's main
message and edits it as the state changes::

    🤖 ToO status
    7DT     ✅ sent 15:54:50 KST
            📥 accepted 06:55 UTC, 36 s after sending — tile T12561, queued for the night of 11-10
            📸 observed — completed 11-11 02:59 UTC (100 s × 3)
    RASA36  ⏳ scheduled for 08:12 UTC (target rises; re-checked before send)
            ✅ sent 08:12 UTC (deferred)

The accepted / observed / not-announced lines come from the telescopes'
alert channels (too/obs_tracker.py). A manual (Slack-button) request shows
its status under its own "ToO Request Submitted" confirmation instead; only
with tracking off does it add a ``✋ manual request sent`` line here.

Per telescope the slot keeps a list of lines:

* an immediate send is one line, edited ``⏳ sending…`` → ``✅ sent`` / ``⚠️ failed``;
* a deferred send keeps its ``⏳ scheduled`` line and *appends* each outcome
  (sent / not sent / failed / rescheduled) beneath it.

A slot can be created before its event thread exists (auto-ToO is dispatched
ahead of the Slack post): state accumulates silently and is flushed to Slack
by :meth:`ToOSlot.attach` once the main message's ``thread_ts`` is known.
Slots are persisted by :class:`ToOSlotStore` so a deferred outcome can still
update the message after a bot restart.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

SENDING_LINE = ":hourglass_flowing_sand: sending…"
_INDENT = "   "  # em-spaces: continuation lines under a telescope name


class ToOSlotStore:
    """Tiny JSON file keyed by event ``thread_ts`` — slot message ts + lines.

    Entries older than ``max_age_days`` (judged from the thread_ts epoch) are
    pruned on write so the file cannot grow without bound.
    """

    def __init__(self, path: str, max_age_days: float = 30.0) -> None:
        self._path = path
        self._max_age = max_age_days * 86400.0
        self._lock = threading.Lock()

    def _load(self) -> Dict[str, Any]:
        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) else {}
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as exc:
            logger.warning(f"ToO slot store unreadable ({self._path}): {exc}")
            return {}

    def get(self, thread_ts: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._load().get(thread_ts)

    def find_thread(self, telescope: str, key: str) -> Optional[str]:
        """thread_ts of the slot tagged with a deferred ``key`` for *telescope*."""
        with self._lock:
            for thread_ts, entry in self._load().items():
                if (entry.get("keys") or {}).get(telescope) == key:
                    return thread_ts
        return None

    def put(self, thread_ts: str, entry: Dict[str, Any]) -> None:
        with self._lock:
            data = self._load()
            data[thread_ts] = entry
            cutoff = time.time() - self._max_age
            for key in list(data):
                try:
                    if float(key) < cutoff:
                        del data[key]
                except ValueError:
                    pass
            tmp = f"{self._path}.tmp"
            try:
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump(data, fh, indent=1)
                os.replace(tmp, self._path)
            except OSError as exc:
                logger.error(f"Could not persist ToO slot ({self._path}): {exc}")


class ToOSlot:
    """The editable ``🤖 ToO status`` message of one event thread."""

    def __init__(self, slack: Any, store: Optional[ToOSlotStore] = None) -> None:
        self._slack = slack
        self._store = store
        self._lock = threading.RLock()
        self._attached = False
        self._thread_ts: Optional[str] = None
        self._slot_ts: Optional[str] = None
        self._lines: Dict[str, List[str]] = {}
        # telescope -> "facility|trigger" of its pending deferred ToO, so the
        # slot can be found again when the ASCII row is gone (retraction).
        self._keys: Dict[str, str] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def attach(self, thread_ts: Optional[str]) -> None:
        """Bind to the event thread (``None`` → channel) and flush any state.

        Idempotent: the first call wins. Restores a previously persisted slot
        for this thread so later outcomes edit the existing message.
        """
        with self._lock:
            if self._attached:
                return
            self._attached = True
            self._thread_ts = thread_ts or None
            if self._thread_ts and self._store:
                saved = self._store.get(self._thread_ts)
                if saved:
                    self._slot_ts = saved.get("slot_ts") or None
                    self._keys = {**(saved.get("keys") or {}), **self._keys}
                    # Lines recorded since creation are newer than the saved
                    # ones for any telescope they touch; keep the rest.
                    merged = {t: list(v) for t, v in (saved.get("lines") or {}).items()}
                    merged.update(self._lines)
                    self._lines = merged
            self._flush()

    @classmethod
    def for_thread(cls, slack: Any, store: Optional[ToOSlotStore], thread_ts: Optional[str]) -> "ToOSlot":
        """Convenience: a slot already attached to an existing thread."""
        slot = cls(slack, store)
        slot.attach(thread_ts)
        return slot

    # ------------------------------------------------------------------
    # State changes (thread-safe; each triggers a post/edit once attached)
    # ------------------------------------------------------------------

    def tag_deferred(self, telescope: str, key: str) -> None:
        """Record the deferred-ToO key for *telescope* (persisted with the slot)."""
        with self._lock:
            self._keys[telescope] = key
            self._persist()

    def begin_send(self, telescope: str) -> None:
        """Immediate send dispatched → append the transient ``sending…`` line."""
        with self._lock:
            self._lines.setdefault(telescope, []).append(SENDING_LINE)
            self._flush()

    def finish_send(self, telescope: str, text: str) -> None:
        """Replace the ``sending…`` line with the result (append if absent)."""
        with self._lock:
            lines = self._lines.setdefault(telescope, [])
            for i in range(len(lines) - 1, -1, -1):
                if lines[i] == SENDING_LINE:
                    lines[i] = text
                    break
            else:
                lines.append(text)
            self._flush()

    def add_line(self, telescope: str, text: str) -> None:
        """Append a line (scheduled / deferred outcome) under the telescope."""
        with self._lock:
            self._lines.setdefault(telescope, []).append(text)
            self._flush()

    # ------------------------------------------------------------------
    # Rendering / Slack I/O
    # ------------------------------------------------------------------

    def _render(self) -> List[Dict[str, Any]]:
        out = [":robot_face: *ToO status*"]
        for telescope, lines in self._lines.items():
            if not lines:
                continue
            out.append(f"*{telescope}* {lines[0]}")
            out.extend(f"{_INDENT}{line}" for line in lines[1:])
        return [{"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(out)}}]

    def _flush(self) -> None:
        """Post the slot (first time) or edit it. Caller holds the lock."""
        if not self._attached or not any(self._lines.values()):
            return
        blocks = self._render()
        text = "ToO status"
        resp = None
        if self._slot_ts:
            resp = self._slack.update_message(ts=self._slot_ts, blocks=blocks, text=text)
            if resp is None:
                logger.warning("ToO slot edit failed — posting a fresh status message")
                self._slot_ts = None
        if not self._slot_ts:
            if self._thread_ts:
                resp = self._slack.send_thread_message(
                    thread_ts=self._thread_ts, blocks=blocks, text=text
                )
            else:
                resp = self._slack.send_message(blocks=blocks, text=text)
            self._slot_ts = (resp or {}).get("ts") or None
        self._persist()

    def _persist(self) -> None:
        if self._store and self._thread_ts and self._slot_ts:
            self._store.put(
                self._thread_ts,
                {"slot_ts": self._slot_ts, "lines": self._lines, "keys": self._keys},
            )
