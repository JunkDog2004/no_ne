"""
data_loader.py — Load and unify customer order data from CSV/Excel files
                 and/or a relational database.

Expected output schema (DataFrame):
    customer_id  | str / int
    order_date   | datetime
    order_value  | float
"""

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

import config

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise column names and types."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Accept common column name variants
    rename_map = {
        "customer": "customer_id",
        "cust_id":  "customer_id",
        "date":     "order_date",
        "purchase_date": "order_date",
        "amount":   "order_value",
        "revenue":  "order_value",
        "total":    "order_value",
        "price":    "order_value",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    required = {"customer_id", "order_date", "order_value"}
    missing  = required - set(df.columns)
    if missing:
        raise ValueError(f"Data is missing required columns: {missing}")

    df["order_date"]  = pd.to_datetime(df["order_date"], errors="coerce")
    df["order_value"] = pd.to_numeric(df["order_value"], errors="coerce")
    df["customer_id"] = df["customer_id"].astype(str)

    # Drop rows with nulls in critical fields
    before = len(df)
    df = df.dropna(subset=["customer_id", "order_date", "order_value"])
    df = df[df["order_value"] > 0]
    dropped = before - len(df)
    if dropped:
        logger.warning(f"Dropped {dropped} invalid rows during normalisation.")

    return df.reset_index(drop=True)


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_from_csv(filepath: str | Path) -> pd.DataFrame:
    """Load orders from a CSV or Excel file."""
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    if filepath.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(filepath)
    else:
        df = pd.read_csv(filepath)

    logger.info(f"Loaded {len(df)} rows from {filepath.name}")
    return _normalise(df)


def load_from_database(
    connection_url: str | None = None,
    query: str | None = None,
) -> pd.DataFrame:
    """Load orders from a SQL database via SQLAlchemy."""
    url   = connection_url or config.DATABASE_URL
    query = query          or config.DB_ORDERS_QUERY

    engine = create_engine(url)
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    logger.info(f"Loaded {len(df)} rows from database.")
    return _normalise(df)


def load_all(
    use_csv: bool = True,
    use_db:  bool = True,
    csv_path: str | None = None,
) -> pd.DataFrame:
    """
    Load from all configured sources and concatenate.
    Deduplicates on (customer_id, order_date, order_value).
    """
    frames = []

    if use_csv:
        path = csv_path or config.CSV_ORDERS_FILE
        try:
            frames.append(load_from_csv(path))
        except FileNotFoundError:
            logger.warning(f"CSV file not found at {path}, skipping.")

    if use_db:
        try:
            frames.append(load_from_database())
        except Exception as e:
            logger.warning(f"DB load failed ({e}), skipping.")

    if not frames:
        raise RuntimeError("No data loaded — check your CSV path and DB connection.")

    combined = pd.concat(frames, ignore_index=True)
    before   = len(combined)
    combined = combined.drop_duplicates(subset=["customer_id", "order_date", "order_value"])
    logger.info(f"Combined: {before} rows → {len(combined)} after dedup.")
    return combined


# ── Sample data generator (for testing without real data) ────────────────────

def generate_sample_data(n_customers: int = 500, seed: int = 42) -> pd.DataFrame:
    """
    Generate synthetic order data so you can run the full pipeline
    without a real dataset.  Saves to data/sample/orders.csv.
    """
    import numpy as np
    rng = np.random.default_rng(seed)

    today = pd.Timestamp.today().normalize()
    rows  = []

    for i in range(n_customers):
        cid    = f"CUST_{i+1:04d}"
        # Each customer has 1–25 orders
        n_orders = int(rng.integers(1, 26))
        # Last order was 1–400 days ago
        last_days_ago = int(rng.integers(1, 401))

        for j in range(n_orders):
            offset = last_days_ago + int(rng.integers(0, 180)) * j
            date   = today - pd.Timedelta(days=offset)
            value  = round(float(rng.lognormal(mean=6.5, sigma=1.0)), 2)
            rows.append({"customer_id": cid, "order_date": date, "order_value": value})

    df = pd.DataFrame(rows)
    out = Path(config.CSV_ORDERS_FILE)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    logger.info(f"Sample data written to {out} ({len(df)} rows, {n_customers} customers).")
    return _normalise(df)
