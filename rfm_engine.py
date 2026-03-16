"""
rfm_engine.py — Compute RFM (Recency, Frequency, Monetary) scores
               for every customer in the orders DataFrame.

Output columns added to the customer-level DataFrame:
    recency_days     | int    – days since last order
    frequency        | int    – total orders in window
    monetary         | float  – total spend in window
    r_score          | int    – 1–5 (5 = best)
    f_score          | int    – 1–5
    m_score          | int    – 1–5
    rfm_score        | float  – weighted composite (1–5)
    rfm_score_str    | str    – e.g. "445" raw RFM string (useful for lookup tables)
"""

import logging
from datetime import datetime

import numpy as np
import pandas as pd

import config

logger = logging.getLogger(__name__)


def compute_rfm(
    orders: pd.DataFrame,
    snapshot_date: datetime | pd.Timestamp | None = None,
    analysis_window_days: int = 365,
) -> pd.DataFrame:
    """
    Compute RFM table from raw orders.

    Parameters
    ----------
    orders : DataFrame with columns [customer_id, order_date, order_value]
    snapshot_date : reference date (default: today)
    analysis_window_days : how far back to look (default 365 days)

    Returns
    -------
    DataFrame indexed by customer_id with RFM scores.
    """
    snap = pd.Timestamp(snapshot_date or datetime.today()).normalize()
    cutoff = snap - pd.Timedelta(days=analysis_window_days)

    # Filter to analysis window
    df = orders[orders["order_date"] >= cutoff].copy()
    if df.empty:
        raise ValueError("No orders in the analysis window. Check your data dates.")

    logger.info(f"RFM window: {cutoff.date()} → {snap.date()}  ({len(df)} orders)")

    # Aggregate per customer
    rfm = (
        df.groupby("customer_id")
        .agg(
            last_order_date=("order_date", "max"),
            frequency=("order_id" if "order_id" in df.columns else "order_value", "count"),
            monetary=("order_value", "sum"),
        )
        .reset_index()
    )

    rfm["recency_days"] = (snap - rfm["last_order_date"]).dt.days

    # Score each dimension
    rfm["r_score"] = _bin_score(
        rfm["recency_days"],
        config.RECENCY_BINS,
        config.RECENCY_LABELS,
        ascending=False,   # lower recency_days = better
    )
    rfm["f_score"] = _bin_score(
        rfm["frequency"],
        config.FREQUENCY_BINS,
        config.FREQUENCY_LABELS,
        ascending=True,
    )
    rfm["m_score"] = _bin_score(
        rfm["monetary"],
        config.MONETARY_BINS,
        config.MONETARY_LABELS,
        ascending=True,
    )

    # Weighted composite
    w = config.RFM_WEIGHTS
    rfm["rfm_score"] = (
        rfm["r_score"] * w["recency"]
        + rfm["f_score"] * w["frequency"]
        + rfm["m_score"] * w["monetary"]
    ).round(2)

    rfm["rfm_score_str"] = (
        rfm["r_score"].astype(str)
        + rfm["f_score"].astype(str)
        + rfm["m_score"].astype(str)
    )

    rfm["monetary"] = rfm["monetary"].round(2)
    rfm = rfm.drop(columns=["last_order_date"])

    logger.info(
        f"RFM computed for {len(rfm)} customers. "
        f"Score range: {rfm['rfm_score'].min():.2f} – {rfm['rfm_score'].max():.2f}"
    )
    return rfm


def _bin_score(
    series: pd.Series,
    bins: list[float],
    labels: list[int],
    ascending: bool = True,
) -> pd.Series:
    """Map continuous values into integer score bins."""
    scored = pd.cut(series, bins=bins, labels=labels, include_lowest=True)
    return scored.astype(int)
