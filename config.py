"""
config.py — Central project configuration.
Edit the values in this file (or use a .env file) to connect your data sources.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent

# ── Data sources ─────────────────────────────────────────────────────────────
# CSV / Excel input files (relative to BASE_DIR/data/)
CSV_ORDERS_FILE   = os.getenv("CSV_ORDERS_FILE",   "data/sample/orders.csv")
CSV_CUSTOMERS_FILE = os.getenv("CSV_CUSTOMERS_FILE", "data/sample/customers.csv")

# Database connection string (SQLAlchemy format)
# Examples:
#   sqlite:///data/customers.db
#   postgresql://user:pass@localhost:5432/mydb
#   mysql+pymysql://user:pass@localhost/mydb
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data/customers.db")

# SQL query to pull raw order data from your DB (customize columns as needed)
DB_ORDERS_QUERY = """
    SELECT customer_id, order_date, order_value
    FROM orders
    WHERE order_date >= date('now', '-2 years')
"""

# ── RFM weights ───────────────────────────────────────────────────────────────
RFM_WEIGHTS = {"recency": 0.40, "frequency": 0.35, "monetary": 0.25}

# Recency scoring breakpoints (days; lower = more recent = higher score)
RECENCY_BINS   = [0, 14, 30, 60, 120, 99999]   # edges
RECENCY_LABELS = [5, 4, 3, 2, 1]               # scores

# Frequency scoring breakpoints (number of orders in analysis window)
FREQUENCY_BINS   = [0, 1, 3, 7, 15, 99999]
FREQUENCY_LABELS = [1, 2, 3, 4, 5]

# Monetary scoring breakpoints (total spend in analysis window)
MONETARY_BINS   = [0, 500, 2000, 6000, 15000, 9999999]
MONETARY_LABELS = [1, 2, 3, 4, 5]

# ── Churn signal weights (0–1, must sum to 1.0) ───────────────────────────────
CHURN_WEIGHTS = {
    "recency_gap":      0.30,   # days since last order vs personal average
    "frequency_drop":   0.25,   # order frequency decline vs prior period
    "spend_drop":       0.20,   # spend decline vs prior period
    "support_issues":   0.15,   # open / unresolved support tickets
    "email_engagement": 0.10,   # email open/click rate drop
}

# Churn risk tiers
CHURN_TIERS = {
    "low":      (0,  30),
    "amber":    (31, 60),
    "high":     (61, 80),
    "critical": (81, 100),
}

# ── Pipeline schedule ─────────────────────────────────────────────────────────
SCHEDULE_INTERVAL_HOURS = int(os.getenv("SCHEDULE_HOURS", "24"))

# ── API ───────────────────────────────────────────────────────────────────────
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

# ── Outputs ───────────────────────────────────────────────────────────────────
OUTPUT_DIR    = BASE_DIR / "outputs"
REPORT_HTML   = OUTPUT_DIR / "segment_report.html"
EXPORT_CSV    = OUTPUT_DIR / "segments_latest.csv"
SEGMENTS_DB   = BASE_DIR  / "data" / "segments.db"
