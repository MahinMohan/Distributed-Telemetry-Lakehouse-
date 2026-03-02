import os
import time
import requests
import pandas as pd
import streamlit as st

API_BASE = os.getenv("TELEMETRY_API_BASE", "http://localhost:8080")
REFRESH_SEC = float(os.getenv("REFRESH_SEC", "1.0"))

def get_json(path, params=None):
    url = f"{API_BASE.rstrip('/')}/{path.lstrip('/')}"
    r = requests.get(url, params=params, timeout=5)
    r.raise_for_status()
    return r.json()

def to_df(series, ts_key="ts_ms", value_key="value"):
    if not series:
        return pd.DataFrame(columns=["ts", "value"])
    df = pd.DataFrame(series)
    if ts_key in df.columns:
        df["ts"] = pd.to_datetime(df[ts_key], unit="ms", utc=True).dt.tz_convert("America/Los_Angeles").dt.tz_localize(None)
    elif "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_convert("America/Los_Angeles").dt.tz_localize(None)
    else:
        df["ts"] = pd.NaT
    if value_key in df.columns:
        df["value"] = pd.to_numeric(df[value_key], errors="coerce")
    elif "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    else:
        df["value"] = pd.NA
    return df[["ts", "value"]].sort_values("ts")

st.set_page_config(page_title="Telemetry Lakehouse", layout="wide")

if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = True
if "refresh_sec" not in st.session_state:
    st.session_state.refresh_sec = REFRESH_SEC

with st.sidebar:
    st.title("Telemetry")
    api_base_input = st.text_input("API Base", API_BASE)
    st.session_state.refresh_sec = st.slider("Refresh (sec)", 0.2, 10.0, float(st.session_state.refresh_sec), 0.2)
    st.session_state.auto_refresh = st.toggle("Auto refresh", value=st.session_state.auto_refresh)
    time_range_min = st.selectbox("Window", ["5m", "15m", "1h", "6h", "24h"], index=2)
    metric = st.selectbox("Metric", ["ingest_rate_msgs", "ingest_rate_bytes", "produce_rate_msgs", "produce_rate_bytes", "end_to_end_p50_ms", "end_to_end_p95_ms", "delivery_errors", "dropped"], index=0)
    host = st.text_input("Host filter (optional)", "")
    topic = st.text_input("Topic filter (optional)", "")
    st.divider()
    st.caption("Expected endpoints:")
    st.code("/health\n/metrics/summary\n/metrics/timeseries?metric=...&window=...")

API_BASE = api_base_input

left, right = st.columns([1.2, 1.0], gap="large")

with left:
    st.subheader("System health")
    health_ok = False
    health_payload = None
    health_error = None
    try:
        health_payload = get_json("/health")
        health_ok = bool(health_payload.get("ok", True))
    except Exception as e:
        health_error = str(e)

    if health_error:
        st.error(f"API unreachable: {health_error}")
    else:
        if health_ok:
            st.success("Healthy")
        else:
            st.warning("Degraded")
        st.json(health_payload)

    st.subheader("Summary")
    summary_error = None
    summary = {}
    try:
        summary = get_json("/metrics/summary", params={"window": time_range_min, "host": host or None, "topic": topic or None})
    except Exception as e:
        summary_error = str(e)

    if summary_error:
        st.error(f"Failed to load summary: {summary_error}")
    else:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Ingest msg/s", f"{summary.get('ingest_rate_msgs', 0):,.0f}")
        k2.metric("Ingest MiB/s", f"{summary.get('ingest_rate_mib', 0):,.2f}")
        k3.metric("Produce msg/s", f"{summary.get('produce_rate_msgs', 0):,.0f}")
        k4.metric("Produce MiB/s", f"{summary.get('produce_rate_mib', 0):,.2f}")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("E2E p50 (ms)", f"{summary.get('end_to_end_p50_ms', 0):,.0f}")
        k6.metric("E2E p95 (ms)", f"{summary.get('end_to_end_p95_ms', 0):,.0f}")
        k7.metric("Dropped", f"{summary.get('dropped', 0):,}")
        k8.metric("Delivery errors", f"{summary.get('delivery_errors', 0):,}")

        if "notes" in summary and summary["notes"]:
            st.info(summary["notes"])

with right:
    st.subheader("Timeseries")
    ts_error = None
    series = []
    try:
        resp = get_json("/metrics/timeseries", params={"metric": metric, "window": time_range_min, "host": host or None, "topic": topic or None})
        series = resp.get("series", resp if isinstance(resp, list) else [])
    except Exception as e:
        ts_error = str(e)

    if ts_error:
        st.error(f"Failed to load timeseries: {ts_error}")
    else:
        df = to_df(series, ts_key="ts_ms", value_key="value")
        if df.empty:
            st.warning("No data in the selected window.")
        else:
            st.line_chart(df.set_index("ts")["value"], height=320)
            with st.expander("Raw points"):
                st.dataframe(df, use_container_width=True, height=260)

    st.subheader("Recent events")
    events_error = None
    events = []
    try:
        resp = get_json("/events/recent", params={"limit": 50, "window": time_range_min, "host": host or None, "topic": topic or None})
        events = resp.get("events", resp if isinstance(resp, list) else [])
    except Exception as e:
        events_error = str(e)

    if events_error:
        st.warning("Events endpoint unavailable.")
    else:
        if events:
            ev = pd.DataFrame(events)
            if "ts_ms" in ev.columns:
                ev["ts"] = pd.to_datetime(ev["ts_ms"], unit="ms", utc=True).dt.tz_convert("America/Los_Angeles").dt.tz_localize(None)
                cols = ["ts"] + [c for c in ev.columns if c not in {"ts", "ts_ms"}]
                st.dataframe(ev[cols].sort_values("ts", ascending=False), use_container_width=True, height=260)
            else:
                st.dataframe(ev, use_container_width=True, height=260)
        else:
            st.caption("No recent events.")

st.caption(f"API: {API_BASE} | Refresh: {st.session_state.refresh_sec:.1f}s")

if st.session_state.auto_refresh:
    time.sleep(st.session_state.refresh_sec)
    st.rerun()
