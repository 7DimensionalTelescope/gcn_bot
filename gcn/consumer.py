"""
GCNConsumer
===========
Wraps the ``gcn_kafka.Consumer`` with:

* Topic subscription and main-loop polling.
* Heartbeat-based connection monitoring in a background thread.
* Automatic reconnection with exponential back-off.

Usage::

    consumer = GCNConsumer(gcn_id, gcn_secret, topics, connection_timeout=300)
    consumer.start_connection_monitor(
        on_lost=lambda: ...,
        on_restored=lambda: ...,
    )
    consumer.start(on_message=my_callback)  # blocks until stop() is called
"""

import logging
import threading
import time
from datetime import datetime
from typing import Any, Callable, List, Optional

logger = logging.getLogger(__name__)


class GCNConsumer:
    """
    GCN Kafka consumer with built-in connection monitoring and reconnection.

    Parameters
    ----------
    client_id : str
        GCN client ID.
    client_secret : str
        GCN client secret.
    topics : list[str]
        Kafka topics to subscribe to (heartbeat is added automatically).
    connection_timeout : int
        Seconds without a heartbeat before the connection is considered lost.
    """

    HEARTBEAT_TOPIC = "gcn.heartbeat"
    MAX_RECONNECT_ATTEMPTS = 5
    BASE_RECONNECT_DELAY   = 30   # seconds

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        topics: List[str],
        connection_timeout: int = 300,
    ) -> None:
        self._client_id      = client_id
        self._client_secret  = client_secret
        self._topics         = topics
        self._timeout        = connection_timeout

        self._consumer       = None
        self._consumer_lock  = threading.Lock()
        self._running        = False
        self._heartbeat_lock = threading.Lock()
        self._last_heartbeat = datetime.now()
        self._last_connected = True
        self._reconnect_attempts = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self, on_message: Callable[[str, Any], None]) -> None:
        """
        Subscribe to topics and begin the polling loop (blocks until ``stop()``).

        Parameters
        ----------
        on_message : callable
            Called for every non-heartbeat message with signature
            ``on_message(topic: str, value: bytes)``.
        """
        self._running = True
        self._consumer = self._create_consumer()
        if self._consumer is None:
            logger.error("Could not create GCN consumer — aborting")
            return

        all_topics = list(dict.fromkeys(self._topics + [self.HEARTBEAT_TOPIC]))
        self._consumer.subscribe(all_topics)
        logger.info(f"Subscribed to {len(all_topics)} topics")

        while self._running:
            try:
                msg = self._consumer.poll(timeout=3.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Kafka consumer error: {msg.error()}")
                    if self._running:
                        self._do_reconnect()
                    continue

                topic = msg.topic()
                value = msg.value()

                if topic == self.HEARTBEAT_TOPIC:
                    self._update_heartbeat()
                else:
                    try:
                        on_message(topic, value)
                    except Exception as exc:
                        logger.error(f"on_message callback raised: {exc}", exc_info=True)

            except Exception as exc:
                logger.error(f"Error in polling loop: {exc}", exc_info=True)
                if self._running:
                    time.sleep(5)

    def stop(self) -> None:
        """Signal the polling loop to exit and close the consumer."""
        self._running = False
        with self._consumer_lock:
            consumer = self._consumer
            self._consumer = None
        if consumer is not None:
            t = threading.Thread(target=consumer.close, daemon=True)
            t.start()
            t.join(timeout=10)
            if t.is_alive():
                logger.warning("Consumer.close() timed out after 10 s — skipping")
        logger.info("GCNConsumer stopped")

    def start_connection_monitor(
        self,
        on_lost: Callable[[], None],
        on_restored: Callable[[], None],
    ) -> threading.Thread:
        """
        Start a background daemon thread that calls *on_lost* / *on_restored*
        when the connection status changes.

        Returns the thread object (already started).
        """
        t = threading.Thread(
            target=self._monitor_loop,
            args=(on_lost, on_restored),
            daemon=True,
            name="gcn-connection-monitor",
        )
        t.start()
        logger.info("Connection monitor thread started")
        return t

    def trigger_reconnection(self) -> None:
        """Force the monitor to think the connection is stale immediately."""
        with self._heartbeat_lock:
            self._last_heartbeat = datetime.fromtimestamp(0)

    # ------------------------------------------------------------------
    # Private: heartbeat
    # ------------------------------------------------------------------

    def _update_heartbeat(self) -> None:
        with self._heartbeat_lock:
            self._last_heartbeat = datetime.now()
            logger.debug("Heartbeat updated")

    def _is_connected(self) -> bool:
        with self._heartbeat_lock:
            elapsed = (datetime.now() - self._last_heartbeat).total_seconds()
            return elapsed < self._timeout

    def _elapsed_since_heartbeat(self) -> float:
        with self._heartbeat_lock:
            return (datetime.now() - self._last_heartbeat).total_seconds()

    def _last_heartbeat_time(self) -> datetime:
        with self._heartbeat_lock:
            return self._last_heartbeat

    # ------------------------------------------------------------------
    # Private: connection monitor loop
    # ------------------------------------------------------------------

    def _monitor_loop(
        self,
        on_lost: Callable[[], None],
        on_restored: Callable[[], None],
    ) -> None:
        while self._running:
            connected = self._is_connected()

            if connected != self._last_connected:
                if not connected:
                    logger.warning("Connection lost — triggering on_lost callback")
                    try:
                        on_lost()
                    except Exception as exc:
                        logger.error(f"on_lost callback error: {exc}")
                else:
                    self._reconnect_attempts = 0
                    logger.info("Connection restored — triggering on_restored callback")
                    try:
                        on_restored()
                    except Exception as exc:
                        logger.error(f"on_restored callback error: {exc}")

                self._last_connected = connected

            # Check more frequently when disconnected
            sleep_s = 30 if not connected else 60
            time.sleep(sleep_s)

    # ------------------------------------------------------------------
    # Private: reconnection
    # ------------------------------------------------------------------

    def _do_reconnect(self) -> None:
        """Called from the polling loop on a Kafka error."""
        if self._reconnect_attempts >= self.MAX_RECONNECT_ATTEMPTS:
            logger.error("Max reconnection attempts reached — giving up")
            self._running = False
            return

        delay = min(self.BASE_RECONNECT_DELAY * (2 ** self._reconnect_attempts), 600)
        self._reconnect_attempts += 1
        logger.info(f"Reconnection attempt {self._reconnect_attempts} in {delay}s…")
        time.sleep(delay)

        new_consumer = self._create_consumer()
        if new_consumer is None:
            logger.error("Failed to create replacement consumer")
            return

        all_topics = list(dict.fromkeys(self._topics + [self.HEARTBEAT_TOPIC]))
        try:
            new_consumer.subscribe(all_topics)
            # Test: poll once
            msg = new_consumer.poll(timeout=5.0)
            with self._consumer_lock:
                if self._consumer:
                    try:
                        self._consumer.close()
                    except Exception:
                        pass
                self._consumer = new_consumer
            self._update_heartbeat()
            logger.info("Reconnection successful")
        except Exception as exc:
            logger.error(f"Reconnection failed: {exc}")
            try:
                new_consumer.close()
            except Exception:
                pass

    def _create_consumer(self):
        """Instantiate a fresh ``gcn_kafka.Consumer``."""
        try:
            from gcn_kafka import Consumer
            consumer = Consumer(
                client_id=self._client_id,
                client_secret=self._client_secret,
            )
            logger.info("gcn_kafka.Consumer created")
            return consumer
        except Exception as exc:
            logger.error(f"Failed to create gcn_kafka.Consumer: {exc}")
            return None
