"""
STREAMING source: an MQTT subscriber that lands live log events in the lake.

WHY MQTT?
---------
MQTT is the lightweight publish/subscribe protocol that IoT devices, edge agents
and log forwarders speak.  A broker (we run Eclipse Mosquitto in Docker) sits in
the middle: producers publish to a topic, consumers subscribe to it, and neither
knows about the other.

HOW THIS FITS THE MEDALLION PICTURE
-----------------------------------
This process is deliberately *dumb*.  It does not clean, join or aggregate.  It
subscribes, buffers, and writes files -- because the value of a landing zone is
that ingestion can never be blocked by transformation logic.  If a downstream
transform has a bug, events keep landing and you reprocess later.

MICRO-BATCHING
--------------
Writing one file per message would produce millions of tiny files, and a lake full
of tiny files is slow to query and expensive to list.  So we buffer in memory and
flush when EITHER:
    * the buffer reaches STREAM_BATCH_SIZE messages  (throughput bound), or
    * STREAM_FLUSH_SECONDS have elapsed              (latency bound)
That pair of conditions is the standard micro-batch trade-off, and both knobs are
in `config.py` so you can feel the difference by changing them.

DELIVERY GUARANTEES
-------------------
We subscribe at QoS 1 ("at least once"), so the broker may redeliver a message.
We do not fight that here -- we make the SILVER layer idempotent instead
(de-duplication on event_id).  "At-least-once transport + idempotent processing"
is how you get effectively-exactly-once without a distributed transaction.
"""

from __future__ import annotations

import json
import signal
import threading
import time
import uuid
from datetime import datetime, timezone
from types import FrameType

import paho.mqtt.client as mqtt

from ..config import get_settings
from ..logging_utils import configure_logging

LOGGER = configure_logging("stream-ingestor")


class MicroBatchWriter:
    """Buffers messages in memory and flushes them to the landing zone as JSONL."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.settings.ensure_directories()
        self._buffer: list[str] = []
        self._lock = threading.Lock()  # on_message runs on the paho network thread
        self._last_flush = time.monotonic()
        self.files_written = 0
        self.messages_received = 0

    def add(self, payload: str) -> None:
        with self._lock:
            self._buffer.append(payload)
            self.messages_received += 1
            should_flush = len(self._buffer) >= self.settings.stream_batch_size
        if should_flush:
            self.flush("size")

    def maybe_flush_on_time(self) -> None:
        """Called from the main loop once a second to honour the latency bound."""
        due = (time.monotonic() - self._last_flush) >= self.settings.stream_flush_seconds
        if due:
            self.flush("time")

    def flush(self, trigger: str = "manual") -> None:
        with self._lock:
            if not self._buffer:
                self._last_flush = time.monotonic()
                return
            batch, self._buffer = self._buffer, []

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        name = f"stream-{stamp}-{uuid.uuid4().hex[:8]}.jsonl"
        # Write to `.tmp` first, then rename.  Bronze skips `.tmp` files, so the
        # promotion job can never pick up a half-written batch.  Rename is atomic.
        tmp_path = self.settings.landing_stream / f"{name}.tmp"
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write("\n".join(batch) + "\n")
        tmp_path.rename(self.settings.landing_stream / name)

        self._last_flush = time.monotonic()
        self.files_written += 1
        LOGGER.info("flushed %d messages -> %s (trigger=%s)", len(batch), name, trigger)


def _on_connect(client: mqtt.Client, userdata, flags, reason_code, properties=None) -> None:
    settings = get_settings()
    if reason_code == 0:
        # Subscribe inside on_connect, not once at startup: if the broker restarts,
        # paho reconnects automatically and this re-establishes the subscription.
        client.subscribe(settings.mqtt_topic, qos=settings.mqtt_qos)
        LOGGER.info("connected to mqtt://%s:%s, subscribed to %s",
                    settings.mqtt_host, settings.mqtt_port, settings.mqtt_topic)
    else:
        LOGGER.error("mqtt connection failed: %s", reason_code)


def _on_message(client: mqtt.Client, userdata: MicroBatchWriter, message: mqtt.MQTTMessage) -> None:
    """Runs on paho's network thread -- keep it short, never do I/O-heavy work."""
    try:
        payload = message.payload.decode("utf-8")
    except UnicodeDecodeError:
        LOGGER.warning("dropping non-utf8 message on topic %s", message.topic)
        return
    # Do NOT validate here.  Bronze's contract is "capture as received"; malformed
    # payloads are supposed to reach the lake so they can be inspected later.
    userdata.add(payload)


def run_ingestor() -> None:
    """Connect, subscribe and land messages until the container is stopped."""
    settings = get_settings()
    writer = MicroBatchWriter()

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"{settings.mqtt_client_id}-{uuid.uuid4().hex[:6]}",
        # clean_session=False + a stable client id would let the broker queue
        # messages while we are down.  We keep it simple here and rely on the
        # producer/broker retaining recent traffic.
    )
    client.user_data_set(writer)
    client.on_connect = _on_connect
    client.on_message = _on_message
    # paho retries the initial connect too, so a slow-starting broker in
    # docker-compose does not crash the container.
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    stopping = threading.Event()

    def _shutdown(signum: int, frame: FrameType | None) -> None:
        # Flush on SIGTERM so `docker compose down` never loses a partial buffer.
        LOGGER.info("signal %s received, flushing and exiting", signum)
        stopping.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    _connect_with_retry(client, settings.mqtt_host, settings.mqtt_port)
    client.loop_start()  # background network thread

    try:
        while not stopping.is_set():
            writer.maybe_flush_on_time()
            time.sleep(1)
    finally:
        client.loop_stop()
        client.disconnect()
        writer.flush("shutdown")
        LOGGER.info(
            "ingestor stopped: %d messages, %d files",
            writer.messages_received, writer.files_written,
        )


def _connect_with_retry(client: mqtt.Client, host: str, port: int, attempts: int = 30) -> None:
    """Wait for the broker container to accept connections before giving up."""
    for attempt in range(1, attempts + 1):
        try:
            client.connect(host, port, keepalive=60)
            return
        except OSError as exc:
            LOGGER.warning("mqtt not reachable (attempt %d/%d): %s", attempt, attempts, exc)
            time.sleep(min(2 * attempt, 10))
    raise RuntimeError(f"could not connect to MQTT broker at {host}:{port}")


if __name__ == "__main__":  # `python -m pipeline.sources.mqtt_stream`
    run_ingestor()
