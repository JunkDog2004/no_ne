"""
api.py — FastAPI REST API to query segments and trigger the pipeline.

Start the server:
    uvicorn api:app --host 0.0.0.0 --port 8000 --reload

Endpoints:
    GET  /health                     – health check
    GET  /segments                   – all customers with segments
    GET  /segments/{customer_id}     – one customer's profile
    GET  /segments/by-name/{name}    – all customers in a segment
    GET  /churn/high-risk            – customers with churn_tier = high or critical
    GET  /summary                    – segment size counts + avg RFM
    POST /pipeline/run               – trigger a pipeline run (async)
"""

import asyncio
import logging
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from pipeline import run_pipeline, load_latest_from_db

logger = logging.getLogger(__name__)
app = FastAPI(title="Customer Segmentation API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Simple in-memory cache
_cache: dict = {"df": None}


def _get_df() -> pd.DataFrame:
    """Return cached segment data, loading from DB if needed."""
    if _cache["df"] is None:
        _cache["df"] = load_latest_from_db()
    if _cache["df"] is None:
        raise HTTPException(
            status_code=503,
            detail="No segment data available yet. POST /pipeline/run to generate it.",
        )
    return _cache["df"]


# ── Models ────────────────────────────────────────────────────────────────────

class CustomerProfile(BaseModel):
    customer_id:    str
    recency_days:   int
    frequency:      int
    monetary:       float
    r_score:        int
    f_score:        int
    m_score:        int
    rfm_score:      float
    segment:        str
    action:         str
    churn_score:    int
    churn_tier:     str


class SegmentSummary(BaseModel):
    segment:        str
    count:          int
    pct:            float
    avg_rfm:        float
    avg_monetary:   float
    avg_churn:      float


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/segments", response_model=list[CustomerProfile])
def get_all_segments(
    limit: int  = Query(100, le=5000),
    offset: int = Query(0, ge=0),
    segment: Optional[str] = Query(None, description="Filter by segment name"),
    churn_tier: Optional[str] = Query(None, description="Filter by churn tier"),
):
    df = _get_df()
    if segment:
        df = df[df["segment"].str.lower() == segment.lower()]
    if churn_tier:
        df = df[df["churn_tier"].str.lower() == churn_tier.lower()]
    page = df.iloc[offset : offset + limit]
    return page[list(CustomerProfile.model_fields.keys())].to_dict(orient="records")


@app.get("/segments/{customer_id}", response_model=CustomerProfile)
def get_customer(customer_id: str):
    df  = _get_df()
    row = df[df["customer_id"] == customer_id]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Customer '{customer_id}' not found.")
    return row.iloc[0][list(CustomerProfile.model_fields.keys())].to_dict()


@app.get("/segments/by-name/{segment_name}", response_model=list[CustomerProfile])
def get_by_segment(segment_name: str, limit: int = Query(200, le=2000)):
    df   = _get_df()
    rows = df[df["segment"].str.lower() == segment_name.lower()].head(limit)
    if rows.empty:
        raise HTTPException(status_code=404, detail=f"No customers in segment '{segment_name}'.")
    return rows[list(CustomerProfile.model_fields.keys())].to_dict(orient="records")


@app.get("/churn/high-risk", response_model=list[CustomerProfile])
def get_high_risk(limit: int = Query(100, le=1000)):
    df   = _get_df()
    rows = (
        df[df["churn_tier"].isin(["high", "critical"])]
        .sort_values("churn_score", ascending=False)
        .head(limit)
    )
    return rows[list(CustomerProfile.model_fields.keys())].to_dict(orient="records")


@app.get("/summary", response_model=list[SegmentSummary])
def get_summary():
    df    = _get_df()
    total = len(df)
    out   = []
    for seg, group in df.groupby("segment"):
        out.append(SegmentSummary(
            segment       = seg,
            count         = len(group),
            pct           = round(len(group) / total * 100, 1),
            avg_rfm       = round(group["rfm_score"].mean(), 2),
            avg_monetary  = round(group["monetary"].mean(), 2),
            avg_churn     = round(group["churn_score"].mean(), 1),
        ))
    return sorted(out, key=lambda x: x.count, reverse=True)


@app.post("/pipeline/run")
async def trigger_pipeline(use_sample: bool = False):
    """Trigger a pipeline run in the background."""
    async def _run():
        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(None, run_pipeline, use_sample)
        _cache["df"] = df

    asyncio.create_task(_run())
    return {"status": "Pipeline started in background. Check /summary in ~30s."}
