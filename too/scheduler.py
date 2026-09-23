"""
DeferredToOScheduler
====================
A tiny background scheduler for *deferred* Target-of-Opportunity sends.

When a GRB is not observable yet but rises soon (the ``observable_soon``
criterion), the ToO is not sent immediately — instead it is registered here and
fired once the target is actually up. At fire time the caller re-checks the
event (still not retracted? latest coordinates still a single tile? observable
now?) before sending, so the request always goes out on the most up-to-date
information.

Design
------
* One daemon thread wakes every ``check_interval`` seconds (or immediately when
  a new entry is scheduled) and invokes ``on_fire(entry)`` for every entry whose
  ``fire_at`` has passed.
* ``on_fire`` runs on the scheduler thread and owns the final re-check + send.
  It may re-``schedule`` the entry (e.g. the target rises a little later than
  first estimated). Exceptions from ``on_fire`` are caught and logged so one bad
  entry never kills the thread.
* The scheduler itself keeps only in-memory state; durability across restarts is
  the caller's job (it persists ``fire_at`` in the ASCII file and re-schedules
  pending entries on startup).
"""

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# (facility, trigger_num, telescope) — the event+telescope identity.
DeferredKey = Tuple[str, str, str]


@dataclass
class DeferredEntry:
    facility: str
    trigger_num: str
    telescope: str
    fire_at: datetime                      # tz-aware UTC
    payload: Dict[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> DeferredKey:
        return (self.facility, self.trigger_num, self.telescope)


class DeferredToOScheduler:
    """Fire ``on_fire(entry)`` when each scheduled entry becomes due."""

    # Floor on the tick interval so a mis-set 0 / negative value can't turn the
    # loop into a CPU-burning busy-wait.
    _MIN_CHECK_INTERVAL = 1.0

    def __init__(
        self,
        on_fire: Callable[[DeferredEntry], None],
        check_interval: float = 30.0,
    ) -> None:
        self._on_fire = on_fire
        try:
            interval = float(check_interval)
        except (TypeError, ValueError):
            interval = 30.0
        if interval < self._MIN_CHECK_INTERVAL:
            logger.warning(
                f"DeferredToOScheduler check_interval={check_interval!r} is below "
                f"the {self._MIN_CHECK_INTERVAL}s floor — clamping (a 0/negative "
                f"value would busy-loop). Set a sane value like 30 in settings.toml."
            )
            interval = self._MIN_CHECK_INTERVAL
        self._check_interval = interval
        self._entries: Dict[DeferredKey, DeferredEntry] = {}
        self._lock    = threading.Lock()
        self._wake    = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._running = False

    # ------------------------------------------------------------------
    # Public API (thread-safe)
    # ------------------------------------------------------------------
    def schedule(self, entry: DeferredEntry) -> None:
        """Add or replace a pending entry and wake the loop."""
        with self._lock:
            self._entries[entry.key] = entry
        logger.info(
            f"Deferred ToO scheduled: {entry.key} at {entry.fire_at.isoformat()}"
        )
        self._wake.set()

    def cancel(self, key: DeferredKey) -> bool:
        """Remove a pending entry. Returns True if one was present."""
        with self._lock:
            existed = self._entries.pop(key, None) is not None
        if existed:
            logger.info(f"Deferred ToO cancelled: {key}")
        return existed

    def is_pending(self, key: DeferredKey) -> bool:
        with self._lock:
            return key in self._entries

    def pending_count(self) -> int:
        with self._lock:
            return len(self._entries)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, name="deferred-too", daemon=True
        )
        self._thread.start()
        logger.info("DeferredToOScheduler started")

    def stop(self) -> None:
        self._running = False
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("DeferredToOScheduler stopped")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    def _loop(self) -> None:
        while self._running:
            now = datetime.now(timezone.utc)
            # Pop everything currently due under the lock, then fire outside it
            # (on_fire may call back into schedule/cancel).
            due = []
            with self._lock:
                for key, entry in list(self._entries.items()):
                    if entry.fire_at <= now:
                        due.append(entry)
                        del self._entries[key]
            for entry in due:
                try:
                    self._on_fire(entry)
                except Exception as exc:
                    logger.error(
                        f"Deferred ToO on_fire failed for {entry.key}: {exc}",
                        exc_info=True,
                    )
            self._wake.wait(self._check_interval)
            self._wake.clear()
