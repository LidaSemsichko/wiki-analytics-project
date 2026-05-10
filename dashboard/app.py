import requests
import pandas as pd
import streamlit as st


API_BASE_URL = "http://api:8000"


st.set_page_config(
    page_title="Wikipedia Analytics Platform",
    page_icon="📊",
    layout="wide",
)


def api_get(path: str, params: dict | None = None):
    try:
        response = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"API request failed: {e}")
        return None


def to_dataframe(data):
    if not data:
        return pd.DataFrame()
    if isinstance(data, dict):
        return pd.DataFrame([data])
    return pd.DataFrame(data)


def render_table(data, empty_message="No data available"):
    df = to_dataframe(data)

    if df.empty:
        st.info(empty_message)
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

    return df


def metric_card(label, value):
    st.metric(label, value if value is not None else "—")


st.markdown(
    """
    <style>
    .main {
        background-color: #f7f8fb;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    h1, h2, h3 {
        color: #111827;
    }

    div[data-testid="stMetric"] {
        background-color: white;
        border: 1px solid #e5e7eb;
        padding: 18px;
        border-radius: 18px;
        box-shadow: 0 8px 24px rgba(15, 23, 42, 0.06);
    }

    div[data-testid="stDataFrame"] {
        background-color: white;
        border-radius: 16px;
    }

    .section-card {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 20px;
        padding: 20px;
        box-shadow: 0 8px 24px rgba(15, 23, 42, 0.06);
        margin-bottom: 18px;
    }

    .small-muted {
        color: #6b7280;
        font-size: 0.95rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


st.title("Wikipedia Analytics Platform")
st.caption("Real-time analytics dashboard for Wikimedia page creation events")


with st.sidebar:
    st.header("Navigation")

    page = st.radio(
        "Choose section",
        [
            "Overview",
            "Language Activity",
            "Bot Activity",
            "Breaking News",
            "Spam Alerts",
            "Search",
        ],
    )

    st.divider()

    st.caption("API status")
    health = api_get("/health")

    if health and health.get("status") == "ok":
        st.success("API is online")
    else:
        st.error("API is not available")

    st.caption(f"API URL: `{API_BASE_URL}`")


if page == "Overview":
    st.subheader("System Overview")

    domains = api_get("/api/domains")
    language = api_get("/api/metrics/language", params={"limit": 100})
    bots = api_get("/api/metrics/bots", params={"limit": 100})
    breaking = api_get("/api/alerts/breaking-news", params={"limit": 100})
    spam = api_get("/api/alerts/spam", params={"limit": 100})

    domains_df = to_dataframe(domains)
    language_df = to_dataframe(language)
    bots_df = to_dataframe(bots)
    breaking_df = to_dataframe(breaking)
    spam_df = to_dataframe(spam)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        metric_card("Tracked domains", len(domains_df) if not domains_df.empty else 0)

    with col2:
        if not language_df.empty and "pages_created" in language_df.columns:
            metric_card("Pages in metric windows", int(language_df["pages_created"].sum()))
        else:
            metric_card("Pages in metric windows", 0)

    with col3:
        metric_card("Breaking alerts", len(breaking_df) if not breaking_df.empty else 0)

    with col4:
        metric_card("Spam alerts", len(spam_df) if not spam_df.empty else 0)

    st.divider()

    left, right = st.columns(2)

    with left:
        st.markdown("### Recent Language Activity")
        render_table(language_df.head(20).to_dict("records") if not language_df.empty else [])

    with right:
        st.markdown("### Recent Bot Activity")
        render_table(bots_df.head(20).to_dict("records") if not bots_df.empty else [])

    st.markdown("### Latest Breaking News Alerts")
    render_table(breaking_df.head(10).to_dict("records") if not breaking_df.empty else [])


elif page == "Language Activity":
    st.subheader("Language Activity Dashboard")
    st.caption("Shows page creation activity grouped by Wikimedia domain and 1-minute windows.")

    col1, col2 = st.columns([2, 1])

    with col1:
        domain_filter = st.text_input("Domain filter", placeholder="for example: en.wikipedia.org")

    with col2:
        limit = st.slider("Limit", 10, 500, 100)

    params = {"limit": limit}
    if domain_filter.strip():
        params["domain"] = domain_filter.strip()

    data = api_get("/api/metrics/language", params=params)
    df = render_table(data)

    if not df.empty and {"domain", "pages_created"}.issubset(df.columns):
        st.markdown("### Pages created by domain")
        chart_df = (
            df.groupby("domain", as_index=False)["pages_created"]
            .sum()
            .sort_values("pages_created", ascending=False)
            .head(20)
        )
        st.bar_chart(chart_df, x="domain", y="pages_created")

    if not df.empty and {"domain", "unique_authors"}.issubset(df.columns):
        st.markdown("### Unique authors by domain")
        chart_df = (
            df.groupby("domain", as_index=False)["unique_authors"]
            .sum()
            .sort_values("unique_authors", ascending=False)
            .head(20)
        )
        st.bar_chart(chart_df, x="domain", y="unique_authors")


elif page == "Bot Activity":
    st.subheader("Bot vs Human Activity")
    st.caption("Shows bot and human page creation metrics by domain.")

    col1, col2 = st.columns([2, 1])

    with col1:
        domain_filter = st.text_input("Domain filter", placeholder="for example: commons.wikimedia.org")

    with col2:
        limit = st.slider("Limit", 10, 500, 100)

    params = {"limit": limit}
    if domain_filter.strip():
        params["domain"] = domain_filter.strip()

    data = api_get("/api/metrics/bots", params=params)
    df = render_table(data)

    if not df.empty and {"domain", "bot_percent"}.issubset(df.columns):
        st.markdown("### Bot percentage by domain")
        chart_df = (
            df.groupby("domain", as_index=False)["bot_percent"]
            .mean()
            .sort_values("bot_percent", ascending=False)
            .head(20)
        )
        st.bar_chart(chart_df, x="domain", y="bot_percent")

    if not df.empty and {"domain", "bot_pages", "human_pages"}.issubset(df.columns):
        st.markdown("### Bot and human page counts")
        chart_df = (
            df.groupby("domain", as_index=False)[["bot_pages", "human_pages"]]
            .sum()
            .sort_values("human_pages", ascending=False)
            .head(20)
        )
        st.bar_chart(chart_df, x="domain", y=["bot_pages", "human_pages"])


elif page == "Breaking News":
    st.subheader("Breaking News Detector")
    st.caption("Keyword burst alerts based on frequent words in newly created page titles.")

    col1, col2 = st.columns([2, 1])

    with col1:
        domain_filter = st.text_input("Domain filter", placeholder="for example: commons.wikimedia.org")

    with col2:
        limit = st.slider("Limit", 10, 500, 100)

    params = {"limit": limit}
    if domain_filter.strip():
        params["domain"] = domain_filter.strip()

    data = api_get("/api/alerts/breaking-news", params=params)
    df = render_table(data, "No breaking news alerts yet")

    if not df.empty and {"keyword", "pages_count"}.issubset(df.columns):
        st.markdown("### Top burst keywords")
        chart_df = (
            df.groupby("keyword", as_index=False)["pages_count"]
            .sum()
            .sort_values("pages_count", ascending=False)
            .head(20)
        )
        st.bar_chart(chart_df, x="keyword", y="pages_count")


elif page == "Spam Alerts":
    st.subheader("Spam and Vandalism Alerts")
    st.caption("Rule-based detection of suspicious page titles.")

    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        domain_filter = st.text_input("Domain filter", placeholder="for example: en.wikipedia.org")

    with col2:
        limit = st.slider("Limit", 10, 500, 100)

    with col3:
        severity_filter = st.selectbox("Severity", ["all", "low", "medium", "high"])

    params = {"limit": limit}
    if domain_filter.strip():
        params["domain"] = domain_filter.strip()

    data = api_get("/api/alerts/spam", params=params)
    df = to_dataframe(data)

    if not df.empty and severity_filter != "all" and "severity" in df.columns:
        df = df[df["severity"] == severity_filter]

    render_table(df.to_dict("records") if not df.empty else [], "No spam alerts yet")

    if not df.empty and "reason" in df.columns:
        st.markdown("### Alert reasons")
        chart_df = (
            df.groupby("reason", as_index=False)
            .size()
            .rename(columns={"size": "count"})
            .sort_values("count", ascending=False)
        )
        st.bar_chart(chart_df, x="reason", y="count")


elif page == "Search":
    st.subheader("Search and Ad-hoc Queries")
    st.caption("Use API-backed search for pages, users, and domains.")

    search_type = st.selectbox(
        "Search type",
        [
            "Page by ID",
            "Pages by User",
            "Pages by Domain",
            "Hourly Report",
            "Editor Patterns",
        ],
    )

    if search_type == "Page by ID":
        page_id = st.text_input("Page ID")

        if st.button("Search page"):
            if not page_id.strip():
                st.warning("Enter page ID")
            else:
                data = api_get(f"/api/pages/{page_id.strip()}")
                render_table(data)

    elif search_type == "Pages by User":
        user_id = st.text_input("User ID")
        limit = st.slider("Limit", 10, 500, 100)

        if st.button("Search user pages"):
            if not user_id.strip():
                st.warning("Enter user ID")
            else:
                data = api_get(f"/api/users/{user_id.strip()}/pages", params={"limit": limit})
                render_table(data)

    elif search_type == "Pages by Domain":
        domain = st.text_input("Domain", placeholder="for example: en.wikipedia.org")
        limit = st.slider("Limit", 10, 500, 100)

        if st.button("Search domain pages"):
            if not domain.strip():
                st.warning("Enter domain")
            else:
                data = api_get(f"/api/domains/{domain.strip()}/pages", params={"limit": limit})
                render_table(data)

    elif search_type == "Hourly Report":
        domain = st.text_input("Domain", placeholder="for example: en.wikipedia.org")
        hours = st.slider("Hours", 1, 24, 6)

        if st.button("Generate hourly report"):
            if not domain.strip():
                st.warning("Enter domain")
            else:
                data = api_get("/api/reports/hourly", params={"domain": domain.strip(), "hours": hours})
                render_table(data)

    elif search_type == "Editor Patterns":
        min_pages = st.slider("Minimum pages", 1, 100, 5)
        limit = st.slider("Raw rows limit", 100, 5000, 1000)

        if st.button("Analyze editors"):
            data = api_get(
                "/api/analytics/editor-patterns",
                params={"min_pages": min_pages, "limit": limit},
            )
            render_table(data)