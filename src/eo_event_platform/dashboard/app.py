"""ASTRAYAN recruiter-facing Version 1.0 dashboard."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import get_args

import pandas as pd
import pydeck as pdk
import streamlit as st

from eo_event_platform.api.models import SourceType
from eo_event_platform.dashboard.client import DashboardApiClient, DashboardApiError

SOURCE_TYPES = list(get_args(SourceType))
SOURCE_COLORS = {
    "NASA_ORIGINAL": [83, 216, 251, 210],
    "NASA_REPLAY": [167, 139, 250, 210],
    "SYNTHETIC_SCALE_TEST": [251, 191, 36, 210],
}


def _style() -> None:
    st.markdown(
        """<style>
        .block-container {max-width: 1480px; padding-top: 1.7rem; padding-bottom: 3rem;}
        [data-testid="stMetric"] {background:#101d2e; border:1px solid #20344d; border-radius:12px; padding:14px 16px;}
        [data-testid="stMetricLabel"] {color:#9bb0c5;}
        .eyebrow {color:#53d8fb; font-size:.78rem; letter-spacing:.13em; text-transform:uppercase; font-weight:700;}
        .hero {font-size:2.2rem; line-height:1.12; font-weight:720; margin:.25rem 0 .35rem;}
        .subtle {color:#9bb0c5; margin-bottom:1.5rem;}
        .status-ok {display:inline-block; color:#7ee787; background:#11291d; border:1px solid #245b38; border-radius:999px; padding:.25rem .65rem; font-weight:650;}
        div[data-testid="stDataFrame"] {border:1px solid #20344d; border-radius:10px; overflow:hidden;}
        </style>""",
        unsafe_allow_html=True,
    )


def _fmt_count(value: int) -> str:
    return f"{value:,}"


def _fmt_time(value: datetime | None) -> str:
    return "Not available" if value is None else value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _activity_dates(first: datetime | None, last: datetime | None, max_days: int) -> tuple[date, date]:
    end = (last or datetime.now(timezone.utc)).date()
    first_date = (first or last or datetime.now(timezone.utc)).date()
    return max(first_date, end - timedelta(days=max_days - 1)), end


def _overview(client: DashboardApiClient) -> tuple[object, object, object] | None:
    with st.spinner("Loading verified platform state…"):
        try:
            readiness = client.readiness()
            status = client.status()
            summary = client.summary()
        except DashboardApiError:
            st.error("The FastAPI serving layer is unavailable. Verify the API and try again.")
            return None

    st.markdown('<span class="status-ok">● PLATFORM READY</span>', unsafe_allow_html=True)
    st.subheader("Mission Overview")
    cols = st.columns(3)
    cols[0].metric("Platform", readiness.status.upper())
    cols[1].metric("Quality gate", status.quality_gate_status)
    cols[2].metric("Last successful run", _fmt_time(status.last_successful_pipeline_run))
    cols = st.columns(3)
    cols[0].metric("API version", status.api_version)
    cols[1].metric("Platform version", status.platform_version)
    cols[2].metric("Data freshness", _fmt_time(status.data_freshness))

    st.subheader("Pipeline Overview")
    cols = st.columns(4)
    cols[0].metric("Original", _fmt_count(summary.original_message_count))
    cols[1].metric("Replay", _fmt_count(summary.replay_message_count))
    cols[2].metric("Synthetic", _fmt_count(summary.synthetic_message_count))
    cols[3].metric("Total events", _fmt_count(summary.event_message_count))
    cols = st.columns(3)
    cols[0].metric("Gold version", status.latest_gold_version)
    cols[1].metric("Latest manifest", status.latest_manifest_id[:8])
    cols[2].metric("Airflow", status.latest_airflow_status)
    st.caption(
        f"{_fmt_count(summary.unique_detection_count)} underlying NASA detections are represented "
        f"separately from {_fmt_count(summary.event_message_count)} event messages."
    )
    return readiness, status, summary


def _daily(client: DashboardApiClient, summary: object) -> None:
    st.subheader("Daily Activity")
    default_start, default_end = _activity_dates(summary.first_activity_time, summary.last_activity_time, 31)
    controls = st.columns([1.3, 1.3, 1])
    start_date = controls[0].date_input("Start date", default_start, key="daily_start")
    end_date = controls[1].date_input("End date", default_end, key="daily_end")
    selected = controls[2].selectbox("Classification", ["All", *SOURCE_TYPES], key="daily_source")
    if start_date > end_date or (end_date - start_date).days > 30:
        st.error("Choose an inclusive activity range of 31 days or fewer.")
        return
    with st.spinner("Loading activity aggregates…"):
        try:
            response = client.daily(start_date, end_date, None if selected == "All" else selected)
        except DashboardApiError:
            st.error("Daily activity could not be loaded from FastAPI.")
            return
    if not response.items:
        st.info("No activity was recorded for the selected filters.")
        return
    frame = pd.DataFrame([item.model_dump(mode="json") for item in response.items])
    chart = frame.pivot_table(index="activity_date", columns="source_type", values="event_message_count", aggfunc="sum").fillna(0)
    st.line_chart(chart, height=330)
    st.caption(f"{len(frame):,} bounded aggregate rows · {response.time_semantics.replace('_', ' ')}")


def _map(client: DashboardApiClient, summary: object) -> None:
    st.subheader("Geospatial Explorer")
    date_start, date_end = _activity_dates(summary.first_activity_time, summary.last_activity_time, 7)
    with st.form("map_filters"):
        first = st.columns(4)
        min_lon = first[0].number_input("West", -180.0, 179.99, -180.0)
        max_lon = first[1].number_input("East", -179.99, 180.0, 180.0)
        min_lat = first[2].number_input("South", -90.0, 89.99, -90.0)
        max_lat = first[3].number_input("North", -89.99, 90.0, 90.0)
        second = st.columns(4)
        start = second[0].date_input("Activity start", date_start, key="map_start")
        end = second[1].date_input("Activity end", date_end, key="map_end")
        source = second[2].selectbox("Source", ["All", *SOURCE_TYPES], key="map_source")
        limit = second[3].select_slider("Maximum points", options=[50, 100, 250, 500], value=250)
        submitted = st.form_submit_button("Load bounded map", type="primary")
    if not submitted:
        st.info("Set a bounded area and activity window, then load the map.")
        return
    if min_lon >= max_lon or min_lat >= max_lat or start > end or (end - start).days > 6:
        st.error("Use ordered coordinates and an activity window no longer than seven days.")
        return
    with st.spinner("Loading bounded geospatial events…"):
        try:
            response = client.bbox(
                min_longitude=min_lon, min_latitude=min_lat, max_longitude=max_lon, max_latitude=max_lat,
                start_time=datetime.combine(start, time.min, timezone.utc),
                end_time=datetime.combine(end + timedelta(days=1), time.min, timezone.utc),
                source_type=None if source == "All" else source, limit=limit,
            )
        except DashboardApiError:
            st.error("Geospatial events could not be loaded from FastAPI.")
            return
    if not response.features:
        st.info("No events matched the selected area, time window, and classification.")
        return
    rows = []
    for feature in response.features:
        event = feature.properties
        rows.append({
            "longitude": feature.geometry.coordinates[0], "latitude": feature.geometry.coordinates[1],
            "classification": event.source_type, "dataset": event.source_dataset,
            "event_id": event.event_id, "activity": event.activity_timestamp.isoformat(),
            "color": SOURCE_COLORS[event.source_type],
        })
    frame = pd.DataFrame(rows)
    layer = pdk.Layer(
        "ScatterplotLayer", frame, get_position="[longitude, latitude]", get_fill_color="color",
        get_radius=4500, radius_min_pixels=3, radius_max_pixels=12, pickable=True,
    )
    view = pdk.ViewState(latitude=float(frame.latitude.mean()), longitude=float(frame.longitude.mean()), zoom=2.2)
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view, tooltip={"text": "{classification}\n{dataset}\n{activity}"}), height=470)
    st.caption(f"{len(frame):,} of at most {limit:,} events · cyan original · violet replay · amber synthetic")


def _lineage(client: DashboardApiClient) -> None:
    st.subheader("Detection Lineage")
    with st.form("lineage_search"):
        lineage_id = st.text_input("Lineage root ID", max_chars=256, placeholder="Enter a detection lineage identifier")
        submitted = st.form_submit_button("Trace lineage", type="primary")
    if not submitted:
        st.info("Search for a lineage root to inspect its replay chain and source truth.")
        return
    if not lineage_id.strip():
        st.error("Enter a lineage ID before searching.")
        return
    with st.spinner("Loading detection lineage…"):
        try:
            response = client.lineage(lineage_id)
        except (DashboardApiError, ValueError):
            st.error("The lineage was not found or could not be loaded from FastAPI.")
            return
    summary = response.summary
    cols = st.columns(4)
    cols[0].metric("Messages", _fmt_count(summary.event_message_count))
    cols[1].metric("Original", _fmt_count(summary.original_message_count))
    cols[2].metric("Replay", _fmt_count(summary.replay_message_count))
    cols[3].metric("Synthetic", _fmt_count(summary.synthetic_message_count))
    rows = [{
        "Sequence": event.replay_sequence_number,
        "Classification": event.source_type,
        "Dataset": event.source_dataset,
        "Source record": event.source_record_id,
        "Observation time": _fmt_time(event.event_timestamp),
        "Activity time": _fmt_time(event.activity_timestamp),
        "Parent event": event.parent_event_id,
    } for event in response.events]
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True, height=360)
    if response.next_cursor:
        st.caption("Showing the first 100 events in this bounded lineage response.")


def _health(readiness: object, status: object) -> None:
    st.subheader("Health Panel")
    cols = st.columns(3)
    cols[0].metric("Readiness", readiness.status.upper())
    cols[1].metric("Cache", "ENABLED" if status.cache_enabled else "DISABLED")
    cols[2].metric("Cache TTL", f"{status.cache_ttl_seconds:g}s")
    cols = st.columns(3)
    cols[0].metric("Cache entries", "N/A" if status.cache_entries is None else str(status.cache_entries))
    cols[1].metric("Latest Airflow run", status.latest_airflow_run_id)
    cols[2].metric("Quality gate", status.quality_gate_status)


def render_dashboard(client: DashboardApiClient | None = None) -> None:
    st.set_page_config(page_title="ASTRAYAN | Earth Observation Intelligence", page_icon="◉", layout="wide", initial_sidebar_state="collapsed")
    _style()
    st.markdown('<div class="eyebrow">NASA Earth Observation Event Intelligence</div>', unsafe_allow_html=True)
    st.markdown('<div class="hero">ASTRAYAN Mission Control</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtle">Governed batch, streaming, geospatial, and lineage intelligence—served exclusively through FastAPI.</div>', unsafe_allow_html=True)
    api = client or DashboardApiClient()
    loaded = _overview(api)
    if loaded is None:
        return
    readiness, status, summary = loaded
    overview_tab, map_tab, lineage_tab = st.tabs(["Overview", "Geospatial", "Detection Lineage"])
    with overview_tab:
        _daily(api, summary)
        _health(readiness, status)
    with map_tab:
        _map(api, summary)
    with lineage_tab:
        _lineage(api)


if __name__ == "__main__":
    render_dashboard()
