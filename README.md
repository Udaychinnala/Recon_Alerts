# QDI Centralized Discrepancy Monitor

## Architecture
```
OneDrive Excel (Power Automate appends rows)
       │
       ▼  [Watermark: last row index stored in SQLite]
  parser.py  — parse HTML tables from raw_html column
       │
       ▼
 alert_engine.py  — classify each row:
   CRITICAL  : |pct| ≥ 5%              → always capture
   HIGH      : 1.5% ≤ |pct| < 5%      → only if first OR worsening
   NORMAL    : |pct| < 1.5%            → auto-resolve open alerts
       │
       ▼
 database.py (SQLite)
   ├── fact_table      (every parsed row, 13K+ rows)
   └── alert_history   (deduped alert lifecycle)
       │
       ▼
   app.py (Streamlit dashboard)
   ├── Tab 1: Overview — current alert state + daily trend
   ├── Tab 2: Alert History — full lifecycle table
   ├── Tab 3: Breach Visibility — ≥5% red table + ≥1.5% amber
   └── Tab 4: Historical Validation — per-table line chart
```

## Setup
```bash
pip install -r requirements.txt

# Edit ONEDRIVE_PATH in app.py to match your local sync path
# Default: C:\Users\006736\OneDrive - Wabtec Corporation\DataDog_Alerting\Reconcilation Alerts.xlsx

streamlit run app.py
```

## Alert Logic
| Condition | Action |
|-----------|--------|
| `|pct| ≥ 5%` | CRITICAL — always captured |
| `1.5% ≤ |pct| < 5%` + first occurrence | HIGH — captured |
| `1.5% ≤ |pct| < 5%` + `curr_gap > prev_gap` | HIGH — captured (worsening) |
| `1.5% ≤ |pct| < 5%` + `curr_gap ≤ prev_gap` | HIGH in fact, NOT in alert history |
| `|pct| < 1.5%` | NORMAL — auto-resolves open alerts |

## Files
- `app.py`          — Streamlit dashboard
- `parser.py`       — HTML table parser (handles all email types)
- `database.py`     — SQLite: fact_table + alert_history + watermark
- `alert_engine.py` — Alert classification engine
- `qdi_monitor.db`  — Pre-bootstrapped SQLite database (370 emails, 13K rows)
- `requirements.txt`— Python dependencies
