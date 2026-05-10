CREATE KEYSPACE IF NOT EXISTS wiki_analytics
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE wiki_analytics;

CREATE TABLE IF NOT EXISTS raw_pages (
    domain text,
    created_at timestamp,
    page_id bigint,
    page_title text,
    user_id bigint,
    user_name text,
    is_bot boolean,
    title_length int,
    PRIMARY KEY ((domain), created_at, page_id)
) WITH CLUSTERING ORDER BY (created_at DESC);

CREATE TABLE IF NOT EXISTS pages_by_user (
    user_id bigint,
    created_at timestamp,
    page_id bigint,
    domain text,
    page_title text,
    user_name text,
    is_bot boolean,
    PRIMARY KEY ((user_id), created_at, page_id)
) WITH CLUSTERING ORDER BY (created_at DESC);

CREATE TABLE IF NOT EXISTS pages_by_id (
    page_id bigint PRIMARY KEY,
    domain text,
    created_at timestamp,
    page_title text,
    user_id bigint,
    user_name text,
    is_bot boolean,
    title_length int
);

CREATE TABLE IF NOT EXISTS language_activity (
    domain text,
    window_start timestamp,
    window_end timestamp,
    pages_created int,
    unique_authors int,
    avg_title_length double,
    trend text,
    PRIMARY KEY ((domain), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS bot_activity_metrics (
    domain text,
    window_start timestamp,
    window_end timestamp,
    bot_pages int,
    human_pages int,
    bot_percent double,
    top_bots text,
    top_humans text,
    PRIMARY KEY ((domain), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS breaking_news_alerts (
    domain text,
    alert_time timestamp,
    alert_type text,
    keyword text,
    spike_ratio double,
    pages_count int,
    sample_pages text,
    PRIMARY KEY ((domain), alert_time)
) WITH CLUSTERING ORDER BY (alert_time DESC);

CREATE TABLE IF NOT EXISTS spam_alerts (
    domain text,
    alert_time timestamp,
    severity text,
    user_id bigint,
    user_name text,
    reason text,
    page_title text,
    page_id bigint,
    PRIMARY KEY ((domain), alert_time, page_id)
) WITH CLUSTERING ORDER BY (alert_time DESC);