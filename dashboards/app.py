"""Streamlit dashboard for SF311: daily ETL, key metrics, trends, and quick refresh."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from datetime import date
from datetime import timedelta
from typing import Optional, Tuple

import pandas as pd
import streamlit as st
from scripts.db import ENGINE

st.set_page_config(page_title="SF311 — City Operations Pulse", layout="wide")
st.markdown("""
    <h1 style="font-size:2.6rem; font-weight:800; margin-bottom:0.3rem; color:white; line-height:1.2; word-wrap:break-word;">
        San Francisco 311 — City Operations Pulse
    </h1>
    <p style="color:#bbb; font-size:1.1rem; margin-top:0.2rem;">
        Tracking San Francisco's 311 requests to see what's being reported, how quickly it's handled, and which neighborhoods stay busiest.

    </p>
""", unsafe_allow_html=True)

st.markdown("""
<style>
  .kpi-row {
      display: flex;
      justify-content: center;
      gap: 5rem;
      text-align: center;
      margin-top: 1.5rem;
      margin-bottom: 0.5rem;
  }
  .kpi {
      font-size: 2rem;
      font-weight: 700;
      margin: 0;
      text-align: center;
  }
  .kpi-sub {
      color: #888;
      margin-top: -6px;
      text-align: center;
      font-size: 1rem;
  }
  .block-container {padding-top: 3rem !important; padding-bottom: 1rem;}
  .control-note { color:#9aa0a6; margin-top:-8px; margin-bottom:6px; text-align:center; }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)
def q(sql: str, params: Optional[dict] = None, _nonce: int = 0) -> pd.DataFrame:
    with ENGINE.connect() as c:
        return pd.read_sql(sql, c, params=params or {})

@st.cache_data(ttl=300)
def get_date_bounds(_nonce: int = 0) -> Tuple[date, date]:
    df = q("SELECT MIN(date(created_at)) AS min_d, MAX(date(created_at)) AS max_d FROM sf311", _nonce=_nonce)
    return df.loc[0, "min_d"], df.loc[0, "max_d"]

@st.cache_data(ttl=300)
def get_categories(_nonce: int = 0) -> list[str]:
    df = q("SELECT DISTINCT category FROM sf311 WHERE category IS NOT NULL AND category <> '' ORDER BY 1", _nonce=_nonce)
    return df["category"].dropna().tolist()

@st.cache_data(ttl=300)
def get_last_created_at(_nonce: int = 0) -> pd.Timestamp | None:
    df = q("SELECT MAX(created_at) AS last_ts FROM sf311", _nonce=_nonce)
    return df.loc[0, "last_ts"]

def empty_state(msg: str):
    st.info(msg)
    st.stop()

nonce = st.session_state.get("_refresh_nonce", 0)

min_d, max_d = get_date_bounds(nonce)
if pd.isna(min_d) or pd.isna(max_d):
    empty_state("No data available. Did you run the load step?")

# --- Controls (top, compact expander) ---
with st.expander("Filters & export", expanded=False):
    st.caption("Adjust filters, refresh, or export without leaving the page.")
    sp_l, c1, c2, cR, sp_r = st.columns([0.1, 1.5, 1.3, 0.8, 0.1], gap="medium")
    with c1:
        dr = st.date_input("Date range", value=(min_d, max_d), min_value=min_d, max_value=max_d)
        if isinstance(dr, tuple):
            start_d, end_d = dr
        else:
            start_d, end_d = min_d, max_d
    with c2:
        cats = get_categories(nonce)
        sel_cats = st.multiselect("Category", cats, default=[])

    # build query scope used by the whole app
    params = {"start_d": str(start_d), "end_d": str(end_d)}
    where = "created_at >= %(start_d)s AND created_at < (%(end_d)s::date + INTERVAL '1 day')"
    if sel_cats:
        where += " AND category = ANY(%(cats)s)"
        params["cats"] = sel_cats

    with cR:
        if st.button("Refresh data", use_container_width=True):
            st.session_state["_refresh_nonce"] = nonce + 1
            st.cache_data.clear()
            st.rerun()
        # space between buttons
        st.markdown("<div style='margin-top:1.2rem;'></div>", unsafe_allow_html=True)
        slice_df = q(f"""
            SELECT request_id, created_at, closed_at, status, category, subcategory, neighborhood
            FROM sf311
            WHERE {where}
            ORDER BY created_at DESC
            LIMIT 100000
        """, params, _nonce=nonce)
        if not slice_df.empty:
            csv_bytes = slice_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Download CSV",
                data=csv_bytes,
                file_name=f"sf311_{start_d}_{end_d}.csv",
                mime="text/csv",
                use_container_width=True
            )

st.markdown("")  # minimal spacing under expander

daily = q(f"""
    SELECT date(created_at) AS day, COUNT(*) AS requests
    FROM sf311
    WHERE {where}
    GROUP BY 1 ORDER BY 1
""", params, _nonce=nonce)

by_neighborhood = q(f"""
    SELECT neighborhood, COUNT(*) AS requests
    FROM sf311
    WHERE {where}
      AND neighborhood IS NOT NULL AND neighborhood <> ''
    GROUP BY neighborhood
    ORDER BY requests DESC
    LIMIT 15
""", params, _nonce=nonce)

metrics = q(f"""
    WITH base AS (
        SELECT created_at, closed_at, status, category
        FROM sf311
        WHERE {where}
    )
    SELECT
      COUNT(*)::bigint                                         AS total_requests,
      SUM(CASE WHEN status ILIKE 'open%%' THEN 1 ELSE 0 END)   AS open_count,
      PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (closed_at - created_at))/3600
      )                                                         AS median_resolve_hours
    FROM base
""", params, _nonce=nonce)

recent = q(f"""
    SELECT request_id, created_at, status, category, neighborhood
    FROM sf311
    WHERE {where}
    ORDER BY created_at DESC
    LIMIT 25
""", params, _nonce=nonce)

if daily.empty:
    empty_state("No rows match your filters. Try widening the date range or clearing categories.")

last_ts = get_last_created_at(nonce)
if pd.notna(last_ts):
    st.caption(f"Data through: {pd.to_datetime(last_ts).strftime('%Y-%m-%d %H:%M %Z')}")

total_req = int(metrics.loc[0, "total_requests"] or 0)
open_count = int(metrics.loc[0, "open_count"] or 0)
open_rate = (open_count / total_req * 100) if total_req else 0
med_hours = metrics.loc[0, "median_resolve_hours"]
med_hours_txt = f"{med_hours:.1f} h" if pd.notna(med_hours) else "—"

st.markdown("<div class='kpi-row'>", unsafe_allow_html=True)
k1, k2, k3 = st.columns(3)
with k1:
    st.markdown(f"<div class='kpi'>{total_req:,}</div>", unsafe_allow_html=True)
    st.markdown("<div class='kpi-sub'>Total requests</div>", unsafe_allow_html=True)
with k2:
    st.markdown(f"<div class='kpi'>{open_rate:.1f}%</div>", unsafe_allow_html=True)
    st.markdown("<div class='kpi-sub'>Open rate</div>", unsafe_allow_html=True)
with k3:
    st.markdown(f"<div class='kpi'>{med_hours_txt}</div>", unsafe_allow_html=True)
    st.markdown("<div class='kpi-sub'>Median resolution time</div>", unsafe_allow_html=True)
st.markdown("</div>", unsafe_allow_html=True)

def _insight_block(delta_val: float, area: str, area_count: int) -> str:
    if delta_val > 0:
        arrow, color = "▲", "#ef4444"  # red for increase
    elif delta_val < 0:
        arrow, color = "▼", "#22c55e"  # green for decrease
    else:
        arrow, color = "—", "#a3a3a3"
    trend = f"{arrow} {abs(delta_val):.1f}% week-over-week" if delta_val != 0 else "Stable vs last week"
    top   = f"{area} ({area_count:,} requests)" if area and area_count else "N/A"
    return f"""
    <div style="border:1px solid #333; border-radius:6px; padding:0.8rem 1rem; background-color:#141414; margin: 0.8rem 0 0.4rem;">
        <strong style="font-size:1.05rem; color:{color};">{trend}</strong><br>
        <span style="color:#ccc;">Highest activity in <b>{top}</b></span>
    </div>
    """

try:
    if len(daily) >= 14:
        trend_now = daily["requests"].iloc[-7:].mean()
        trend_prev = daily["requests"].iloc[-14:-7].mean()
        delta = ((trend_now - trend_prev) / trend_prev * 100) if trend_prev else 0.0
    else:
        delta = 0.0

    if not by_neighborhood.empty:
        top_area = str(by_neighborhood.iloc[0, 0])
        top_area_count = int(by_neighborhood.iloc[0, 1])
    else:
        top_area, top_area_count = "N/A", 0

    st.markdown("### Notable changes (Week-over-Week)")
    st.markdown(_insight_block(delta, top_area, top_area_count), unsafe_allow_html=True)
except Exception:
    pass

# --- Notable changes (week-over-week deltas) ---
try:
    # need at least 14 days ending at end_d
    if (end_d - start_d).days >= 13:
        curr_start = end_d - timedelta(days=6)
        prev_start = end_d - timedelta(days=13)
        prev_end   = end_d - timedelta(days=7)

        wo_params = {
            "cs": str(curr_start),
            "ce": str(end_d),
            "ps": str(prev_start),
            "pe": str(prev_end),
        }

        by_cat_curr = q("""
            SELECT COALESCE(category,'(unknown)') AS key, COUNT(*)::int AS cnt
            FROM sf311
            WHERE created_at >= %(cs)s AND created_at < (%(ce)s::date + INTERVAL '1 day')
            GROUP BY 1
        """, wo_params, _nonce=nonce).rename(columns={"cnt":"curr"})
        by_cat_prev = q("""
            SELECT COALESCE(category,'(unknown)') AS key, COUNT(*)::int AS cnt
            FROM sf311
            WHERE created_at >= %(ps)s AND created_at < (%(pe)s::date + INTERVAL '1 day')
            GROUP BY 1
        """, wo_params, _nonce=nonce).rename(columns={"cnt":"prev"})
        cat = by_cat_curr.merge(by_cat_prev, on="key", how="outer").fillna(0)
        cat["delta"] = cat["curr"] - cat["prev"]
        cat["pct"] = cat.apply(lambda r: (r["delta"] / r["prev"] * 100) if r["prev"] else float("inf") if r["delta"]>0 else 0.0, axis=1)
        cat = cat.sort_values(["delta","curr"], ascending=[False,False]).head(3)

        by_nb_curr = q("""
            SELECT COALESCE(neighborhood,'(unknown)') AS key, COUNT(*)::int AS cnt
            FROM sf311
            WHERE created_at >= %(cs)s AND created_at < (%(ce)s::date + INTERVAL '1 day')
            GROUP BY 1
        """, wo_params, _nonce=nonce).rename(columns={"cnt":"curr"})
        by_nb_prev = q("""
            SELECT COALESCE(neighborhood,'(unknown)') AS key, COUNT(*)::int AS cnt
            FROM sf311
            WHERE created_at >= %(ps)s AND created_at < (%(pe)s::date + INTERVAL '1 day')
            GROUP BY 1
        """, wo_params, _nonce=nonce).rename(columns={"cnt":"prev"})
        nb = by_nb_curr.merge(by_nb_prev, on="key", how="outer").fillna(0)
        nb["delta"] = nb["curr"] - nb["prev"]
        nb["pct"] = nb.apply(lambda r: (r["delta"] / r["prev"] * 100) if r["prev"] else float("inf") if r["delta"]>0 else 0.0, axis=1)
        nb = nb.sort_values(["delta","curr"], ascending=[False,False]).head(3)

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown(f"Category Trends (Week-over-Week)")
            if not cat.empty:
                st.dataframe(cat.rename(columns={"key":"category","curr":"this week","prev":"last week","delta":"Δ","pct":"Δ%"}), hide_index=True, use_container_width=True)
            else:
                st.caption("No category movement detected.")
        with col_b:
            st.markdown(f"Neighborhood Trends (Week-over-Week)")
            if not nb.empty:
                st.dataframe(nb.rename(columns={"key":"neighborhood","curr":"this week","prev":"last week","delta":"Δ","pct":"Δ%"}), hide_index=True, use_container_width=True)
            else:
                st.caption("No neighborhood movement detected.")
    else:
        st.caption("Not enough history for week-over-week changes (need ≥14 days in the selected range).")
except Exception as _e:
    st.caption("Could not compute week-over-week changes.")

st.divider()

c1, c2 = st.columns((2, 1), gap="large")

with c1:
    st.subheader("Requests per day")
    st.line_chart(daily.set_index("day"), height=300)

with c2:
    st.subheader("Top neighborhoods")
    st.bar_chart(by_neighborhood.set_index("neighborhood"), height=300)

st.subheader("Top categories")
top_cat = q(f"""
    SELECT category, COUNT(*) AS requests
    FROM sf311
    WHERE {where}
      AND category IS NOT NULL AND category <> ''
    GROUP BY category
    ORDER BY requests DESC
    LIMIT 10
""", params, _nonce=nonce)
if not top_cat.empty:
    st.bar_chart(top_cat.set_index("category"), height=300)
else:
    st.caption("No category data in current filter window.")

st.subheader("Status breakdown")
status_counts = q(f"""
    SELECT status, COUNT(*) AS count
    FROM sf311
    WHERE {where}
    GROUP BY status
    ORDER BY count DESC
""", params, _nonce=nonce)
if not status_counts.empty:
    st.bar_chart(status_counts.set_index("status"), height=260)
else:
    st.caption("No status data in current filter window.")

st.divider()

st.subheader("Latest 25 requests")
st.dataframe(recent, use_container_width=True, hide_index=True)

st.divider()
st.caption(f"Connected to: {ENGINE.url}")