"""
pipeline.py — Orchestrates the full segmentation pipeline.

Can be run:
  1. Once manually:   python pipeline.py
  2. On a schedule:   python pipeline.py --schedule
  3. Imported by API: from pipeline import run_pipeline
"""

import argparse
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

import config
from data_loader import load_all, generate_sample_data
from rfm_engine import compute_rfm
from segmenter import assign_segments, compute_churn_score
from reporter import generate_html_report, export_csv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_pipeline(use_sample: bool = False) -> pd.DataFrame:
    """
    Execute the full pipeline end-to-end.

    Steps:
        1. Load raw orders
        2. Compute RFM scores
        3. Assign segments + churn scores
        4. Save to SQLite + CSV
        5. Generate HTML report

    Returns the enriched customer DataFrame.
    """
    logger.info("=" * 60)
    logger.info("Pipeline started")
    start = datetime.now()

    # 1. Load data
    if use_sample:
        logger.info("Using generated sample data.")
        orders = generate_sample_data()
    else:
        orders = load_all(use_csv=True, use_db=True)

    # 2. RFM
    rfm = compute_rfm(orders)

    # 3. Segments + churn
    customers = assign_segments(rfm)
    customers = compute_churn_score(customers)

    # 4. Persist
    _save_to_db(customers)
    export_csv(customers)

    # 5. Report
    generate_html_report(customers, orders)

    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"Pipeline complete in {elapsed:.1f}s. {len(customers)} customers processed.")
    logger.info("=" * 60)
    return customers


def _save_to_db(df: pd.DataFrame) -> None:
    """Upsert the latest segment snapshot into SQLite."""
    config.SEGMENTS_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(config.SEGMENTS_DB)

    df_out = df.copy()
    df_out["updated_at"] = datetime.utcnow().isoformat()

    df_out.to_sql("segments", conn, if_exists="replace", index=False)
    conn.close()
    logger.info(f"Saved {len(df_out)} customer records to {config.SEGMENTS_DB}")


def load_latest_from_db() -> pd.DataFrame | None:
    """Load the most recently computed segment table from SQLite."""
    if not config.SEGMENTS_DB.exists():
        return None
    conn = sqlite3.connect(config.SEGMENTS_DB)
    df = pd.read_sql("SELECT * FROM segments", conn)
    conn.close()
    return df


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Customer segmentation pipeline")
    parser.add_argument("--schedule", action="store_true",
                        help="Run on a recurring schedule instead of once")
    parser.add_argument("--sample", action="store_true",
                        help="Use generated sample data (no real data needed)")
    args = parser.parse_args()

    if args.schedule:
        logger.info(f"Starting scheduler (interval: {config.SCHEDULE_INTERVAL_HOURS}h)")
        scheduler = BlockingScheduler()
        scheduler.add_job(
            lambda: run_pipeline(use_sample=args.sample),
            IntervalTrigger(hours=config.SCHEDULE_INTERVAL_HOURS),
            next_run_time=datetime.now(),   # run immediately on start
        )
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped.")
    else:
        run_pipeline(use_sample=args.sample)
