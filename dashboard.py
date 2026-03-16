"""
dashboard.py — Streamlit interactive dashboard.

Run with:
    streamlit run dashboard.py
"""

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import config
from pipeline import run_pipeline, load_latest_from_db
from segmenter import SEGMENT_RULES

st.set_page_config(
    page_title="Customer Segmentation",
    page_icon="📊",
    layout="wide",
)

# ── Load data ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_data():
    df = load_latest_from_db()
    return df


def main():
    st.title("Customer Segmentation Dashboard")

    # Sidebar controls
    with st.sidebar:
        st.header("Controls")
        if st.button("▶ Run pipeline now"):
            with st.spinner("Running segmentation pipeline…"):
                run_pipeline(use_sample=True)
            st.success("Done! Refresh to see updated data.")
            st.cache_data.clear()

        if st.button("🧪 Generate sample data & run"):
            with st.spinner("Generating sample data and running pipeline…"):
                run_pipeline(use_sample=True)
            st.success("Sample pipeline complete!")
            st.cache_data.clear()

    df = load_data()

    if df is None:
        st.warning("No data found. Use the sidebar to run the pipeline first.")
        st.stop()

    # ── KPI row ───────────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total customers",    f"{len(df):,}")
    c2.metric("Avg RFM score",      f"{df['rfm_score'].mean():.2f}")
    c3.metric("Champions",          int((df["segment"] == "Champions").sum()))
    c4.metric("High / critical risk", int(df["churn_tier"].isin(["high", "critical"]).sum()))
    c5.metric("Avg monetary",       f"₹{df['monetary'].mean():,.0f}")

    st.divider()

    # ── Row 1: Segment breakdown + Churn distribution ─────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Segment distribution")
        seg_counts = df["segment"].value_counts().reset_index()
        seg_counts.columns = ["Segment", "Count"]
        color_map  = {s["name"]: s["color"] for s in SEGMENT_RULES}
        fig = px.bar(
            seg_counts, x="Count", y="Segment", orientation="h",
            color="Segment", color_discrete_map=color_map,
            text="Count",
        )
        fig.update_layout(showlegend=False, margin=dict(l=0, r=0, t=0, b=0), height=320)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Churn risk distribution")
        tier_order  = ["low", "amber", "high", "critical"]
        tier_colors = {"low": "#1D9E75", "amber": "#EF9F27", "high": "#E24B4A", "critical": "#791F1F"}
        tier_counts = df["churn_tier"].value_counts().reindex(tier_order, fill_value=0).reset_index()
        tier_counts.columns = ["Tier", "Count"]
        fig2 = px.pie(
            tier_counts, values="Count", names="Tier",
            color="Tier", color_discrete_map=tier_colors,
            hole=0.45,
        )
        fig2.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=320)
        st.plotly_chart(fig2, use_container_width=True)

    # ── Row 2: RFM scatter ────────────────────────────────────────────────────
    st.subheader("RFM score vs monetary value")
    fig3 = px.scatter(
        df, x="rfm_score", y="monetary",
        color="segment", color_discrete_map=color_map,
        size="frequency", hover_data=["customer_id", "recency_days", "churn_score"],
        opacity=0.7,
    )
    fig3.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=380)
    st.plotly_chart(fig3, use_container_width=True)

    # ── Row 3: Segment table + high-risk table ────────────────────────────────
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Segment summary")
        seg_summary = (
            df.groupby("segment")
            .agg(count=("customer_id","count"),
                 avg_rfm=("rfm_score","mean"),
                 avg_spend=("monetary","mean"),
                 avg_churn=("churn_score","mean"))
            .round(2).reset_index()
            .sort_values("count", ascending=False)
        )
        st.dataframe(seg_summary, use_container_width=True, hide_index=True)

    with col4:
        st.subheader("High & critical churn risk")
        hr = (
            df[df["churn_tier"].isin(["high", "critical"])]
            [["customer_id","segment","rfm_score","churn_score","churn_tier","monetary"]]
            .sort_values("churn_score", ascending=False)
            .head(20)
        )
        st.dataframe(hr, use_container_width=True, hide_index=True)

    # ── Lookup a single customer ───────────────────────────────────────────────
    st.divider()
    st.subheader("Look up a customer")
    cid = st.text_input("Enter customer ID")
    if cid:
        row = df[df["customer_id"] == cid]
        if row.empty:
            st.error(f"Customer '{cid}' not found.")
        else:
            r = row.iloc[0]
            a, b, c, d = st.columns(4)
            a.metric("Segment",     r["segment"])
            b.metric("RFM score",   r["rfm_score"])
            c.metric("Churn score", r["churn_score"])
            d.metric("Churn tier",  r["churn_tier"])
            st.info(f"**Recommended action:** {r['action']}")

    # ── Download ──────────────────────────────────────────────────────────────
    st.divider()
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇ Download full segment CSV", csv, "segments.csv", "text/csv")


if __name__ == "__main__":
    main()
