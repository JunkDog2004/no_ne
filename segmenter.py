"""
segmenter.py — Assign business segment labels and churn risk scores
               to every customer based on their RFM scores.

Segment rules are expressed as simple score thresholds so they are
easy to tune without touching any ML code. Swap in a trained classifier
later if you need higher precision.
"""

import logging

import pandas as pd

import config

logger = logging.getLogger(__name__)


# ── Segment definitions (ordered from best to worst; first match wins) ────────

SEGMENT_RULES = [
    {
        "name":    "Champions",
        "r_min": 4, "f_min": 4, "m_min": 4,
        "action":  "VIP programme + referral ask",
        "color":   "#1D9E75",
    },
    {
        "name":    "Loyal customers",
        "r_min": 3, "f_min": 4, "m_min": 3,
        "action":  "Upsell + exclusive offer",
        "color":   "#5DCAA5",
    },
    {
        "name":    "High-value infrequent",
        "r_min": 3, "f_min": 1, "m_min": 5,
        "action":  "VIP treatment, re-engagement nudge",
        "color":   "#534AB7",
    },
    {
        "name":    "Potential loyalists",
        "r_min": 4, "f_min": 2, "m_min": 2,
        "action":  "Second purchase incentive + education",
        "color":   "#EF9F27",
    },
    {
        "name":    "New customers",
        "r_min": 5, "f_min": 1, "m_min": 1,
        "action":  "Welcome sequence, onboarding",
        "color":   "#378ADD",
    },
    {
        "name":    "Promising",
        "r_min": 4, "f_min": 1, "m_min": 1,
        "action":  "Early loyalty offer",
        "color":   "#85B7EB",
    },
    {
        "name":    "Need attention",
        "r_min": 3, "f_min": 2, "m_min": 2,
        "action":  "Limited-time re-engagement",
        "color":   "#BA7517",
    },
    {
        "name":    "About to sleep",
        "r_min": 2, "f_min": 2, "m_min": 2,
        "action":  "Discount + product spotlight",
        "color":   "#D85A30",
    },
    {
        "name":    "At risk",
        "r_min": 2, "f_min": 3, "m_min": 3,
        "action":  "Personal outreach + win-back offer",
        "color":   "#E24B4A",
    },
    {
        "name":    "Can't lose them",
        "r_min": 1, "f_min": 4, "m_min": 4,
        "action":  "Urgent outreach, account review",
        "color":   "#A32D2D",
    },
    {
        "name":    "Hibernating",
        "r_min": 1, "f_min": 2, "m_min": 2,
        "action":  "Low-cost reactivation campaign",
        "color":   "#F09595",
    },
    {
        "name":    "Lost",
        "r_min": 1, "f_min": 1, "m_min": 1,   # catch-all
        "action":  "Sunset or one final win-back email",
        "color":   "#888780",
    },
]


def _match_segment(row: pd.Series) -> dict:
    for seg in SEGMENT_RULES:
        if (
            row["r_score"] >= seg["r_min"]
            and row["f_score"] >= seg["f_min"]
            and row["m_score"] >= seg["m_min"]
        ):
            return seg
    return SEGMENT_RULES[-1]   # fallback: Lost


def assign_segments(rfm: pd.DataFrame) -> pd.DataFrame:
    """
    Add segment name, recommended action, and display color to the RFM table.
    """
    df = rfm.copy()
    seg_info = df.apply(_match_segment, axis=1).apply(pd.Series)
    df["segment"]        = seg_info["name"]
    df["action"]         = seg_info["action"]
    df["segment_color"]  = seg_info["color"]

    counts = df["segment"].value_counts().to_dict()
    logger.info(f"Segment distribution: {counts}")
    return df


def compute_churn_score(
    df: pd.DataFrame,
    support_tickets: pd.Series | None = None,
    email_engagement: pd.Series | None = None,
) -> pd.DataFrame:
    """
    Compute a 0–100 churn risk score for each customer.

    Uses RFM signals as proxies when external signals (support, email) are absent.
    Optionally accepts a Series of support ticket counts and email engagement scores
    (0–1) indexed by customer_id.

    Adds columns:
        churn_score  – int 0–100
        churn_tier   – str: low / amber / high / critical
    """
    df = df.copy()
    w  = config.CHURN_WEIGHTS

    # Recency signal: normalise recency_days to 0–100
    max_days = df["recency_days"].quantile(0.99).clip(1)
    recency_signal = (df["recency_days"] / max_days * 100).clip(0, 100)

    # Frequency drop: invert f_score (low f = higher risk)
    freq_signal = ((5 - df["f_score"]) / 4 * 100)

    # Spend drop: invert m_score
    spend_signal = ((5 - df["m_score"]) / 4 * 100)

    # Support signal (external or zero)
    if support_tickets is not None:
        sup = df["customer_id"].map(support_tickets).fillna(0)
        support_signal = (sup.clip(0, 5) / 5 * 100)
    else:
        support_signal = pd.Series(0, index=df.index, dtype=float)

    # Email engagement signal (external or zero)
    if email_engagement is not None:
        eng = df["customer_id"].map(email_engagement).fillna(0.5)
        email_signal = ((1 - eng) * 100).clip(0, 100)
    else:
        email_signal = pd.Series(0, index=df.index, dtype=float)

    df["churn_score"] = (
        recency_signal  * w["recency_gap"]
        + freq_signal   * w["frequency_drop"]
        + spend_signal  * w["spend_drop"]
        + support_signal * w["support_issues"]
        + email_signal   * w["email_engagement"]
    ).clip(0, 100).round(0).astype(int)

    df["churn_tier"] = df["churn_score"].apply(_churn_tier)
    logger.info(
        "Churn scores computed. Tier counts: "
        + str(df["churn_tier"].value_counts().to_dict())
    )
    return df


def _churn_tier(score: int) -> str:
    for tier, (lo, hi) in config.CHURN_TIERS.items():
        if lo <= score <= hi:
            return tier
    return "critical"
