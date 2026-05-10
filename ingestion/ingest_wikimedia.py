import json
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


STREAM_URL = "https://stream.wikimedia.org/v2/stream/page-create"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "wiki-page-create"


def create_producer():
    for attempt in range(1, 31):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=5,
            )
            print("[INGESTION] Connected to Kafka", flush=True)
            return producer
        except NoBrokersAvailable:
            print(f"[INGESTION] Kafka not ready, attempt {attempt}/30", flush=True)
            time.sleep(3)

    raise RuntimeError("Could not connect to Kafka")


def parse_timestamp(value):
    if not value:
        return datetime.now(timezone.utc).isoformat()

    # Wikimedia meta.dt usually looks like: 2026-05-10T18:55:10Z
    if isinstance(value, str):
        try:
            return value.replace("Z", "+00:00")
        except Exception:
            return datetime.now(timezone.utc).isoformat()

    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def normalize_event(event):
    meta = event.get("meta", {})
    performer = event.get("performer", {})
    page = event.get("page", {})

    domain = meta.get("domain") or event.get("database") or "unknown"

    page_id = (
        page.get("page_id")
        or page.get("id")
        or event.get("page_id")
        or event.get("page", {}).get("page_id")
    )

    page_title = (
        page.get("page_title")
        or page.get("title")
        or event.get("page_title")
        or event.get("title")
        or ""
    )

    user_id = (
        performer.get("user_id")
        or performer.get("id")
        or event.get("user_id")
        or 0
    )

    user_name = (
        performer.get("user_text")
        or performer.get("user_name")
        or performer.get("name")
        or event.get("user")
        or "unknown"
    )

    is_bot = bool(
        performer.get("user_is_bot")
        or performer.get("is_bot")
        or event.get("bot")
        or False
    )

    created_at = parse_timestamp(meta.get("dt") or event.get("dt") or event.get("timestamp"))

    if page_id is None:
        page_id = abs(hash(f"{domain}:{page_title}:{created_at}")) % 10_000_000_000

    return {
        "domain": str(domain),
        "page_id": int(page_id),
        "page_title": str(page_title),
        "user_id": int(user_id) if user_id is not None else 0,
        "user_name": str(user_name),
        "is_bot": is_bot,
        "created_at": created_at,
        "title_length": len(str(page_title)),
    }


def connect_to_stream():
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": "wiki-analytics-student-project/1.0"
    }

    response = requests.get(
        STREAM_URL,
        headers=headers,
        stream=True,
        timeout=(10, 120),
    )

    print(f"[INGESTION] Wikimedia HTTP status: {response.status_code}", flush=True)

    if response.status_code != 200:
        print(response.text[:500], flush=True)
        raise RuntimeError(f"Wikimedia stream returned status {response.status_code}")

    return response


def main():
    producer = create_producer()

    while True:
        try:
            print("[INGESTION] Connecting to Wikimedia EventStreams...", flush=True)
            response = connect_to_stream()

            current_event_data = []

            for raw_line in response.iter_lines(decode_unicode=True):
                if raw_line is None:
                    continue

                line = raw_line.strip()

                if not line:
                    if current_event_data:
                        data_str = "\n".join(current_event_data)
                        current_event_data = []

                        try:
                            raw_event = json.loads(data_str)
                            event = normalize_event(raw_event)

                            producer.send(TOPIC, key=event["domain"], value=event)
                            producer.flush(timeout=5)

                            print(
                                f"[INGESTION] Sent page: domain={event['domain']} "
                                f"title={event['page_title'][:80]}",
                                flush=True,
                            )

                        except Exception as e:
                            print(f"[INGESTION] Failed to process event: {repr(e)}", flush=True)

                    continue

                if line.startswith(":"):
                    continue

                if line.startswith("data:"):
                    current_event_data.append(line[5:].strip())

        except Exception as e:
            print(f"[INGESTION] Stream connection failed: {repr(e)}", flush=True)
            print("[INGESTION] Reconnecting in 5 seconds...", flush=True)
            time.sleep(5)


if __name__ == "__main__":
    main()