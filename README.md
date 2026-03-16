# Customer Segmentation & Retention Project

A complete, production-ready Python project that handles:
- **Data ingestion** from CSV/Excel files and SQL databases
- **RFM scoring** — Recency, Frequency, Monetary (weighted composite 1–5)
- **Segment tagging** — 12 named segments (Champions → Lost)
- **Churn risk scoring** — 0–100 score with tier alerts
- **Automated pipeline** — runs on a schedule via APScheduler
- **REST API** — FastAPI endpoints to query segments
- **Dashboard** — Streamlit interactive dashboard with Plotly charts
- **Reports** — HTML report + CSV export on every run

---

## Project structure

```
customer_segmentation/
├── config.py          ← All settings (data sources, weights, schedule)
├── data_loader.py     ← Load from CSV/Excel + SQL database
├── rfm_engine.py      ← Compute RFM scores
├── segmenter.py       ← Assign segments + churn scores
├── pipeline.py        ← Orchestrates everything, scheduler
├── api.py             ← FastAPI REST endpoints
├── dashboard.py       ← Streamlit dashboard
├── reporter.py        ← HTML report + CSV export
├── requirements.txt   ← Python dependencies
├── data/
│   └── sample/        ← Auto-generated sample data
└── outputs/
    ├── segments_latest.csv
    └── segment_report.html
```

---

## Quick start (5 minutes)

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run with sample data (no real data needed)
```bash
python pipeline.py --sample
```
This generates 500 synthetic customers, scores them, and writes outputs to `outputs/`.

### 3. Open the dashboard
```bash
streamlit run dashboard.py
```
Visit http://localhost:8501

### 4. Start the API
```bash
uvicorn api:app --reload
```
Visit http://localhost:8000/docs for interactive API docs.

---

## Using your real data

### Option A — CSV / Excel file

Edit `config.py`:
```python
CSV_ORDERS_FILE = "data/my_orders.csv"
```

Your file needs these columns (other names are auto-mapped):

| Column | Aliases accepted |
|--------|-----------------|
| `customer_id` | `customer`, `cust_id` |
| `order_date` | `date`, `purchase_date` |
| `order_value` | `amount`, `revenue`, `total`, `price` |

Then run:
```bash
python pipeline.py
```

### Option B — Database

Edit `config.py`:
```python
DATABASE_URL    = "postgresql://user:pass@localhost:5432/mydb"
DB_ORDERS_QUERY = "SELECT customer_id, order_date, order_value FROM orders"
```

Install the relevant driver:
```bash
pip install psycopg2-binary   # PostgreSQL
pip install pymysql           # MySQL
```

### Option C — Both (recommended)

`load_all()` merges CSV + DB automatically and deduplicates.

---

## Scheduled pipeline

Run every 24 hours (configurable in `config.py`):
```bash
python pipeline.py --schedule
```

Or with sample data for testing:
```bash
python pipeline.py --schedule --sample
```

---

## API endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/segments` | All customers (supports `?segment=Champions&churn_tier=high`) |
| GET | `/segments/{id}` | Single customer profile |
| GET | `/segments/by-name/{segment}` | All customers in a segment |
| GET | `/churn/high-risk` | Customers with churn tier high or critical |
| GET | `/summary` | Segment counts, avg RFM, avg spend |
| POST | `/pipeline/run` | Trigger a pipeline run in the background |

Full interactive docs at http://localhost:8000/docs

---

## Customising segments

Edit the `SEGMENT_RULES` list in `segmenter.py`. Each rule has:
```python
{
    "name":   "My Segment",
    "r_min":  4,   # minimum R score (1–5) to qualify
    "f_min":  3,   # minimum F score
    "m_min":  3,   # minimum M score
    "action": "What to do with this customer",
    "color":  "#hex",
}
```
Rules are evaluated top-to-bottom; the first match wins.

---

## Customising RFM score bins

Edit the bin breakpoints in `config.py`:
```python
RECENCY_BINS   = [0, 14, 30, 60, 120, 99999]   # days since purchase
FREQUENCY_BINS = [0, 1, 3, 7, 15, 99999]       # number of orders
MONETARY_BINS  = [0, 500, 2000, 6000, 15000, 9999999]  # total spend
```
Adjust these to match your business's typical purchase patterns.

---

## Environment variables

Create a `.env` file to override config without editing code:
```env
DATABASE_URL=postgresql://user:pass@localhost/mydb
CSV_ORDERS_FILE=data/orders_2024.csv
SCHEDULE_HOURS=12
API_PORT=8080
```
