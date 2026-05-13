from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, Query, HTTPException
from cassandra.cluster import Cluster
from cassandra.query import dict_factory


CASSANDRA_HOST = "cassandra"
KEYSPACE = "wiki_analytics"

app = FastAPI(
    title="Wikipedia Analytics API",
    description="API for real-time and historical Wikipedia page creation analytics",
    version="1.0.0",
)

cluster = None
session = None


def row_to_dict(row):
    result = dict(row)

    for key, value in result.items():
        if isinstance(value, datetime):
            result[key] = value.isoformat()

    return result


@app.on_event("startup")
def startup_event():
    global cluster, session

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)
    session.row_factory = dict_factory

    print("[API] Connected to Cassandra")


@app.on_event("shutdown")
def shutdown_event():
    global cluster

    if cluster:
        cluster.shutdown()


@app.get("/")
def root():
    return {
        "service": "Wikipedia Analytics API",
        "status": "ok",
        "endpoints": [
            "/api/domains",
            "/api/pages/{page_id}",
            "/api/users/{user_id}/pages",
            "/api/domains/{domain}/pages",
            "/api/reports/hourly",
            "/api/analytics/editor-patterns",
            "/api/metrics/language",
            "/api/metrics/bots",
            "/api/alerts/spam",
        ],
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/domains")
def get_domains(limit: int = Query(50, ge=1, le=200)):
    query = """
        SELECT domain, window_start, window_end, pages_created, unique_authors, avg_title_length, trend
        FROM language_activity
        LIMIT %s
    """

    rows = session.execute(query, (limit,))
    domains = {}

    for row in rows:
        domain = row["domain"]

        if domain not in domains:
            domains[domain] = {
                "domain": domain,
                "latest_window_start": row["window_start"].isoformat() if row["window_start"] else None,
                "latest_window_end": row["window_end"].isoformat() if row["window_end"] else None,
                "pages_created_last_window": row["pages_created"],
                "unique_authors_last_window": row["unique_authors"],
                "avg_title_length": row["avg_title_length"],
                "trend": row["trend"],
            }

    return list(domains.values())


@app.get("/api/pages/{page_id}")
def get_page_details(page_id: int):
    query = """
        SELECT page_id, domain, created_at, page_title, user_id, user_name, is_bot, title_length
        FROM pages_by_id
        WHERE page_id = %s
    """

    row = session.execute(query, (page_id,)).one()

    if not row:
        raise HTTPException(status_code=404, detail="Page not found")

    return row_to_dict(row)


@app.get("/api/users/{user_id}/pages")
def get_pages_by_user(
    user_id: int,
    limit: int = Query(100, ge=1, le=500),
):
    query = """
        SELECT user_id, created_at, page_id, domain, page_title, user_name, is_bot
        FROM pages_by_user
        WHERE user_id = %s
        LIMIT %s
    """

    rows = session.execute(query, (user_id, limit))

    return [row_to_dict(row) for row in rows]


@app.get("/api/domains/{domain}/pages")
def get_pages_by_domain(
    domain: str,
    from_ts: Optional[str] = Query(None, alias="from"),
    to_ts: Optional[str] = Query(None, alias="to"),
    limit: int = Query(100, ge=1, le=500),
):
    if from_ts and to_ts:
        try:
            start = datetime.fromisoformat(from_ts.replace("Z", "+00:00"))
            end = datetime.fromisoformat(to_ts.replace("Z", "+00:00"))
        except Exception:
            raise HTTPException(
                status_code=400,
                detail="Invalid timestamp format. Use ISO format, for example 2026-05-10T19:00:00+00:00",
            )

        query = """
            SELECT domain, created_at, page_id, page_title, user_id, user_name, is_bot, title_length
            FROM raw_pages
            WHERE domain = %s AND created_at >= %s AND created_at <= %s
            LIMIT %s
        """

        rows = session.execute(query, (domain, start, end, limit))

    else:
        query = """
            SELECT domain, created_at, page_id, page_title, user_id, user_name, is_bot, title_length
            FROM raw_pages
            WHERE domain = %s
            LIMIT %s
        """

        rows = session.execute(query, (domain, limit))

    return [row_to_dict(row) for row in rows]


@app.get("/api/metrics/language")
def get_language_metrics(
    domain: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
):
    if domain:
        query = """
            SELECT domain, window_start, window_end, pages_created, unique_authors, avg_title_length, trend
            FROM language_activity
            WHERE domain = %s
            LIMIT %s
        """
        rows = session.execute(query, (domain, limit))
    else:
        query = """
            SELECT domain, window_start, window_end, pages_created, unique_authors, avg_title_length, trend
            FROM language_activity
            LIMIT %s
        """
        rows = session.execute(query, (limit,))

    return [row_to_dict(row) for row in rows]


@app.get("/api/metrics/bots")
def get_bot_metrics(
    domain: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
):
    if domain:
        query = """
            SELECT domain, window_start, window_end, bot_pages, human_pages, bot_percent, top_bots, top_humans
            FROM bot_activity_metrics
            WHERE domain = %s
            LIMIT %s
        """
        rows = session.execute(query, (domain, limit))
    else:
        query = """
            SELECT domain, window_start, window_end, bot_pages, human_pages, bot_percent, top_bots, top_humans
            FROM bot_activity_metrics
            LIMIT %s
        """
        rows = session.execute(query, (limit,))

    return [row_to_dict(row) for row in rows]


@app.get("/api/alerts/spam")
def get_spam_alerts(
    domain: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
):
    if domain:
        query = """
            SELECT domain, alert_time, severity, user_id, user_name, reason, page_title, page_id
            FROM spam_alerts
            WHERE domain = %s
            LIMIT %s
        """
        rows = session.execute(query, (domain, limit))
    else:
        query = """
            SELECT domain, alert_time, severity, user_id, user_name, reason, page_title, page_id
            FROM spam_alerts
            LIMIT %s
        """
        rows = session.execute(query, (limit,))

    return [row_to_dict(row) for row in rows]


@app.get("/api/reports/hourly")
def get_hourly_report(
    domain: str = Query(...),
    hours: int = Query(6, ge=1, le=24),
):
    now = datetime.now(timezone.utc)

    start_time = now - timedelta(hours=hours)

    query = """
        SELECT domain, created_at, page_id, page_title, user_id, user_name, is_bot
        FROM raw_pages
        WHERE domain = %s AND created_at >= %s AND created_at <= %s
        LIMIT 5000
    """

    rows = list(session.execute(query, (domain, start_time, now)))

    buckets = {}

    for row in rows:
        created_at = row["created_at"]

        hour_start = created_at.replace(minute=0, second=0, microsecond=0)
        hour_key = hour_start.isoformat()

        if hour_key not in buckets:
            buckets[hour_key] = {
                "time_start": hour_start.isoformat(),
                "time_end": (hour_start + timedelta(hours=1)).isoformat(),
                "domain": domain,
                "pages_created": 0,
                "unique_authors_set": set(),
                "bot_pages": 0,
                "human_pages": 0,
                "authors": {},
            }

        bucket = buckets[hour_key]
        bucket["pages_created"] += 1
        bucket["unique_authors_set"].add(row["user_id"])

        if row["is_bot"]:
            bucket["bot_pages"] += 1
        else:
            bucket["human_pages"] += 1

        author_name = row["user_name"] or "unknown"

        if author_name not in bucket["authors"]:
            bucket["authors"][author_name] = {
                "name": author_name,
                "pages": 0,
                "is_bot": row["is_bot"],
            }

        bucket["authors"][author_name]["pages"] += 1

    result = []

    for bucket in buckets.values():
        total = bucket["pages_created"]
        bot_percent = (bucket["bot_pages"] / total * 100) if total else 0

        top_authors = sorted(
            bucket["authors"].values(),
            key=lambda item: item["pages"],
            reverse=True,
        )[:10]

        result.append({
            "time_start": bucket["time_start"],
            "time_end": bucket["time_end"],
            "domain": bucket["domain"],
            "pages_created": bucket["pages_created"],
            "unique_authors": len(bucket["unique_authors_set"]),
            "bot_percent": round(bot_percent, 2),
            "top_authors": top_authors,
        })

    return sorted(result, key=lambda item: item["time_start"])


@app.get("/api/analytics/editor-patterns")
def get_editor_patterns(
    min_pages: int = Query(5, ge=1, le=100),
    limit: int = Query(1000, ge=100, le=5000),
):
    query = """
        SELECT domain, created_at, page_id, page_title, user_id, user_name, is_bot
        FROM raw_pages
        LIMIT %s
    """

    rows = list(session.execute(query, (limit,)))

    editors = {}

    for row in rows:
        user_id = row["user_id"]

        if user_id not in editors:
            editors[user_id] = {
                "user_id": user_id,
                "user_name": row["user_name"],
                "is_bot": row["is_bot"],
                "pages": [],
                "domains": set(),
            }

        editors[user_id]["pages"].append(row["created_at"])
        editors[user_id]["domains"].add(row["domain"])

    result = []

    for editor in editors.values():
        pages_count = len(editor["pages"])

        if pages_count < min_pages:
            continue

        sorted_times = sorted(editor["pages"])

        if len(sorted_times) > 1:
            gaps = [
                (sorted_times[i] - sorted_times[i - 1]).total_seconds()
                for i in range(1, len(sorted_times))
            ]
            avg_seconds_between_pages = sum(gaps) / len(gaps)
        else:
            avg_seconds_between_pages = None

        active_hours = sorted(list(set(t.hour for t in sorted_times)))

        result.append({
            "user_id": editor["user_id"],
            "user_name": editor["user_name"],
            "is_bot": editor["is_bot"],
            "pages_created": pages_count,
            "domains_count": len(editor["domains"]),
            "domains": sorted(list(editor["domains"])),
            "active_hours_utc": active_hours,
            "avg_seconds_between_pages": avg_seconds_between_pages,
            "has_domain_specialization": len(editor["domains"]) == 1,
        })

    return sorted(result, key=lambda item: item["pages_created"], reverse=True)



@app.get("/api/alerts/breaking-news")
def get_breaking_news_alerts(
    domain: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
):
    if domain:
        query = """
            SELECT domain, alert_time, alert_type, keyword, spike_ratio, pages_count, sample_pages
            FROM breaking_news_alerts
            WHERE domain = %s
            LIMIT %s
        """
        rows = session.execute(query, (domain, limit))
    else:
        query = """
            SELECT domain, alert_time, alert_type, keyword, spike_ratio, pages_count, sample_pages
            FROM breaking_news_alerts
            LIMIT %s
        """
        rows = session.execute(query, (limit,))

    return [row_to_dict(row) for row in rows]