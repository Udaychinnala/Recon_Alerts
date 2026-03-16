"""
QDI Centralized Discrepancy Monitor — app.py
Complete Streamlit dashboard with:
  • Incremental file watcher (watermark-based, no full re-scan)
  • Fact table + Alert history (SQLite)
  • unstable-only HIGH alert logic
  • CRITICAL auto-capture at ≥5%
  • Auto-resolve when pct < 1.5%
  • 10-minute auto-refresh
"""

import streamlit as st
import pandas as pd
import os, json
from datetime import datetime, timedelta
from pathlib import Path

# ── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="QDI Discrepancy Monitor",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Imports ──────────────────────────────────────────────────────────────────
import sys
sys.path.insert(0, str(Path(__file__).parent))

from database import (init_db, get_watermark, set_watermark, insert_fact_rows,
                      load_fact_table, load_alert_history, get_db_stats,
                      auto_resolve_alerts, export_alerts_to_excel)
from data_parser import parse_new_rows
from alert_engine import classify_and_capture

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AR = True
except ImportError:
    HAS_AR = False

try:
    import plotly.graph_objects as go
    import plotly.express as px
    HAS_PX = True
except ImportError:
    HAS_PX = False

# ── Config ───────────────────────────────────────────────────────────────────
ONEDRIVE_PATH   = r"C:\Users\006736\OneDrive - Wabtec Corporation\DataDog_Alerting\Reconcilation Alerts.xlsx"
SHEET_NAME      = "Sheet1"
REFRESH_MS      = 10 * 60 * 1000

# Local Excel file where alerts are exported on every run (for Power Automate / statistics)
# Change this path to wherever you want the file saved on your machine.
ALERT_EXPORT_PATH = r"C:\Users\006736\Music\New folder\Wabtec_Application\Biops_sheet\DataDog\Reconcolication\files\QDI_Alerts_Export.xlsx"

# ── Init DB ──────────────────────────────────────────────────────────────────
init_db()

# ── Auto-refresh ─────────────────────────────────────────────────────────────
if HAS_AR:
    rc = st_autorefresh(interval=REFRESH_MS, key="qdi_refresh")
else:
    rc = 0

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=JetBrains+Mono:wght@400;500;700&display=swap');

:root {
  --bg:       #070A12;
  --surface:  #0E1320;
  --card:     #141B2D;
  --border:   #1E2D4A;
  --accent:   #00D4FF;
  --green:    #00FF9D;
  --red:      #FF3B5C;
  --amber:    #FFB020;
  --purple:   #A855F7;
  --text:     #E2EAF8;
  --muted:    #6B7FA3;
  --mono:     'JetBrains Mono', monospace;
  --sans:     'Syne', sans-serif;
}

html, body, [class*="css"] {
  font-family: var(--sans);
  background: var(--bg);
  color: var(--text);
}

/* Header */
.page-header {
  padding: 24px 0 20px;
  border-bottom: 1px solid var(--border);
  margin-bottom: 24px;
}
.page-title {
  font-size: 28px; font-weight: 800; letter-spacing: -0.5px;
  color: var(--text);
  display: flex; align-items: center; gap: 10px;
}
.page-title span.accent { color: var(--accent); }
.page-sub {
  font-family: var(--mono); font-size: 11px;
  color: var(--muted); margin-top: 4px; letter-spacing: 1.5px;
}

/* KPI cards */
.kpi-grid { display: grid; grid-template-columns: repeat(6,1fr); gap: 12px; margin-bottom: 24px; }
.kpi {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 12px; padding: 16px 18px;
  position: relative; overflow: hidden;
}
.kpi::before {
  content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
}
.kpi.red::before   { background: var(--red); }
.kpi.amber::before { background: var(--amber); }
.kpi.green::before { background: var(--green); }
.kpi.blue::before  { background: var(--accent); }
.kpi.purple::before{ background: var(--purple); }
.kpi.grey::before  { background: var(--muted); }

.kpi .lbl {
  font-family: var(--mono); font-size: 9px; font-weight: 700;
  text-transform: uppercase; letter-spacing: 2px; color: var(--muted);
  margin-bottom: 8px;
}
.kpi .val {
  font-family: var(--mono); font-size: 34px; font-weight: 700; line-height: 1;
}
.kpi.red .val   { color: var(--red); }
.kpi.amber .val { color: var(--amber); }
.kpi.green .val { color: var(--green); }
.kpi.blue .val  { color: var(--accent); }
.kpi.purple .val{ color: var(--purple); }
.kpi.grey .val  { color: var(--muted); }

.kpi .sub {
  font-family: var(--mono); font-size: 10px; color: var(--muted);
  margin-top: 6px;
}

/* Status bar */
.status-bar {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 8px; padding: 10px 16px; margin-bottom: 20px;
  font-family: var(--mono); font-size: 11px; color: var(--muted);
  display: flex; align-items: center; gap: 24px; flex-wrap: wrap;
}
.status-dot {
  width: 7px; height: 7px; border-radius: 50%;
  display: inline-block; margin-right: 6px; animation: pulse 2s infinite;
}
.dot-green { background: var(--green); }
.dot-red   { background: var(--red); }
.dot-amber { background: var(--amber); }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.35} }

/* Section headers */
.sec-head {
  font-family: var(--mono); font-size: 10px; font-weight: 700;
  text-transform: uppercase; letter-spacing: 2.5px; color: var(--accent);
  padding: 4px 0 10px; border-bottom: 1px solid var(--border);
  margin: 20px 0 14px;
}

/* Alert badges */
.badge {
  display: inline-block; padding: 2px 8px; border-radius: 4px;
  font-family: var(--mono); font-size: 10px; font-weight: 700;
  letter-spacing: 0.5px;
}
.badge-critical { background: #FF3B5C22; color: var(--red); border: 1px solid #FF3B5C44; }
.badge-high     { background: #FFB02022; color: var(--amber); border: 1px solid #FFB02044; }
.badge-normal   { background: #00FF9D11; color: var(--green); border: 1px solid #00FF9D33; }
.badge-open     { background: #FF3B5C11; color: var(--red); }
.badge-resolved { background: #00FF9D11; color: var(--green); }

/* Trend arrows */
.up   { color: var(--red); font-size: 16px; }
.down { color: var(--green); font-size: 16px; }
.flat { color: var(--muted); font-size: 16px; }

/* New alert flash */
.new-flash {
  background: linear-gradient(90deg, #FF3B5C11, #FF3B5C05);
  border: 1px solid #FF3B5C44; border-radius: 8px;
  padding: 10px 16px; margin-bottom: 16px;
  font-family: var(--mono); font-size: 12px; color: var(--red);
}

[data-testid="stSidebar"] {
  background: var(--surface) !important;
  border-right: 1px solid var(--border) !important;
}
[data-testid="stSidebar"] * { color: var(--text) !important; }

.stTabs [data-baseweb="tab"] {
  background: var(--card) !important;
  border: 1px solid var(--border) !important;
  border-radius: 6px !important;
  margin-right: 6px !important;
  font-family: var(--mono) !important;
  font-size: 11px !important;
  color: var(--muted) !important;
}
.stTabs [aria-selected="true"] {
  background: var(--border) !important;
  color: var(--accent) !important;
}

.dataframe-container { border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }

#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# INCREMENTAL PIPELINE
# ═══════════════════════════════════════════════════════════════
def run_pipeline(excel_path: str) -> dict:
    status = dict(
        file_found=False, source_rows=0, new_rows=0,
        new_facts=0, new_critical=0, new_high=0,
        last_run=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        file_mtime=None, error=None
    )

    if not os.path.exists(excel_path):
        status['error'] = f"File not found:\n{excel_path}"
        return status

    status['file_found'] = True
    status['file_mtime'] = datetime.fromtimestamp(
        os.path.getmtime(excel_path)).strftime("%Y-%m-%d %H:%M:%S")

    try:
        prev_wm = get_watermark()
        records, total = parse_new_rows(excel_path, from_row=prev_wm, sheet_name=SHEET_NAME)
        status['source_rows'] = total
        status['new_rows']    = total - prev_wm

        if records:
            alert_stats = classify_and_capture(records)
            insert_fact_rows(records)
            set_watermark(total)
            status['new_facts']    = len(records)
            status['new_critical'] = alert_stats['critical']
            status['new_high']     = alert_stats['high']
        else:
            set_watermark(total)   # advance even if no parseable rows

        # ── Export alerts to local Excel after every run ────────────────
        try:
            export_alerts_to_excel(ALERT_EXPORT_PATH)
        except Exception as ex_err:
            status['export_error'] = str(ex_err)

    except Exception as e:
        status['error'] = str(e)

    return status


with st.sidebar:
    st.markdown("### 📁 Select Excel File")
    excel_file = st.file_uploader("Upload the latest Reconcilation Alerts.xlsx", type=["xlsx"])

if excel_file:
    import tempfile
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    tmp.write(excel_file.read())
    ONEDRIVE_PATH = tmp.name
else:
    st.stop()  # wait until user uploads file

# ═══════════════════════════════════════════════════════════════
# RUN PIPELINE ON EVERY LOAD (no cache)
# ═══════════════════════════════════════════════════════════════
pipeline_status = run_pipeline(ONEDRIVE_PATH)

# ── Load data ─────────────────────────────────────────────────
fact_df  = load_fact_table()
alert_df = load_alert_history()
db_stats = get_db_stats()


# ═══════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("### ⚡ QDI Monitor")
    st.markdown(
        f"<div style='font-family:JetBrains Mono,monospace;font-size:10px;color:#6B7FA3'>"
        f"Data-driven discrepancy monitoring</div>",
        unsafe_allow_html=True
    )
    st.markdown("---")

    fok  = pipeline_status['file_found']
    fclr = "#00FF9D" if fok else "#FF3B5C"
    st.markdown(
        f"<small style='color:{fclr};font-weight:700'>"
        f"{'✅ Source connected' if fok else '❌ File not found'}</small><br>"
        f"<small style='color:#6B7FA3;font-size:10px;word-break:break-all'>{ONEDRIVE_PATH}</small>",
        unsafe_allow_html=True
    )

    if pipeline_status.get('file_mtime'):
        st.markdown(
            f"<small style='color:#6B7FA3'>📝 Modified: {pipeline_status['file_mtime']}</small>",
            unsafe_allow_html=True
        )

    st.markdown("---")

    if st.button("🔄 Force Refresh", width="stretch"):
        st.rerun()

    if st.button("⚠️ Reset Watermark", width="stretch"):
        from database import set_watermark as sw, get_conn
        conn = get_conn()
        conn.execute("UPDATE watermark SET last_row=0"); conn.commit(); conn.close()
        st.rerun()

    st.markdown("---")

    # ── Excel export status ──────────────────────────────────────────────
    if pipeline_status.get('export_error'):
        st.warning(f"⚠️ Excel export failed:\n{pipeline_status['export_error']}")
    else:
        st.markdown(
            "<small style='color:#00FF9D'>📊 Alerts auto-exported to Excel each run</small><br>"
            f"<small style='color:#6B7FA3;font-size:9px;word-break:break-all'>{ALERT_EXPORT_PATH}</small>",
            unsafe_allow_html=True
        )

    if st.button("📥 Export Alerts Now", width="stretch"):
        ok = export_alerts_to_excel(ALERT_EXPORT_PATH)
        if ok:
            st.success("✅ Exported successfully!")
        else:
            st.error("Export failed — check path/permissions")

    st.markdown("---")

    days_back = st.selectbox("View Period", [3, 7, 14, 30], index=1)
    show_jobs = st.multiselect(
        "Filter Jobs",
        options=sorted(fact_df['job'].dropna().unique()) if not fact_df.empty else [],
        default=[]
    )

    st.markdown("---")
    next_t = datetime.now() + timedelta(minutes=10)
    wm     = db_stats.get('watermark')
    wm_row = wm['last_row'] if wm else '?'
    st.markdown(
        f"<div style='font-size:10px;color:#6B7FA3;font-family:JetBrains Mono,monospace'>"
        f"🕐 Auto-refresh every 10 min<br>"
        f"Next: ~{next_t.strftime('%H:%M')}<br>"
        f"Watermark: row #{wm_row}<br>"
        f"Cycle: #{rc}</div>",
        unsafe_allow_html=True
    )
    if not HAS_AR:
        st.warning("Install `streamlit-autorefresh` for auto-refresh")


# ═══════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════
st.markdown("""
<div class="page-header">
  <div class="page-title"> <span class="accent">⚡ QDI DISCREPANCY MONITOR</span> </div>
</div>
""", unsafe_allow_html=True)


# ── Pipeline status bar ───────────────────────────────────────
if pipeline_status.get('error'):
    st.error(f"⚠️ Pipeline error: {pipeline_status['error']}")
else:
    dot = "dot-green" if pipeline_status['file_found'] else "dot-red"
    new_rows = pipeline_status['new_rows']
    nc = pipeline_status['new_critical']
    nh = pipeline_status['new_high']

    alert_flash = ""
    if nc > 0 or nh > 0:
        alert_flash = f"""
        <div class='new-flash'>
          ⚡ {new_rows} new source row(s) → {pipeline_status['new_facts']} fact rows parsed
          &nbsp;|&nbsp; 🔴 {nc} CRITICAL captured &nbsp;|&nbsp; 🟡 {nh} HIGH captured
        </div>
        """

    st.markdown(f"""
    {alert_flash}
    <div class='status-bar'>
      <span><span class='status-dot {dot}'></span>Last sync: <b>{pipeline_status['last_run']}</b></span>
      <span>📄 Source rows: <b>{pipeline_status['source_rows']:,}</b></span>
      <span>🆕 New rows: <b style='color:{"#00D4FF" if new_rows else "#6B7FA3"}'>{new_rows}</b></span>
      <span>📋 Total facts: <b>{db_stats['total_fact_rows']:,}</b></span>
      <span>🚨 Open alerts: <b style='color:#FF3B5C'>{db_stats['open_alerts']}</b></span>
    </div>
    """, unsafe_allow_html=True)


if fact_df.empty:
    st.info("⏳ Waiting for data — connect your OneDrive Excel file.")
    st.stop()


# ── Apply filters ─────────────────────────────────────────────
fact_df['email_received_ts'] = pd.to_datetime(fact_df['email_received_ts'], errors='coerce')
fact_df['run_date']          = pd.to_datetime(fact_df['run_date'], errors='coerce')
cutoff = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=days_back)
cutoff = cutoff.tz_localize(None)
fdf = fact_df[fact_df['email_received_ts'] >= cutoff].copy() if not fact_df['email_received_ts'].isna().all() else fact_df.copy()

if show_jobs:
    fdf = fdf[fdf['job'].isin(show_jobs)]


# ═══════════════════════════════════════════════════════════════
# KPI CARDS
# ═══════════════════════════════════════════════════════════════
open_crit    = db_stats['open_critical']
open_high    = db_stats['open_high']
open_total   = db_stats['open_alerts']
resolved     = db_stats['resolved_alerts']
total_facts  = db_stats['total_fact_rows']
unique_tbls  = fdf['table_name'].nunique() if not fdf.empty else 0

st.markdown(f"""
<div class='kpi-grid'>
  <div class='kpi red'>
    <div class='lbl'>Open Critical</div>
    <div class='val'>{open_crit}</div>
    <div class='sub'>≥5% breach</div>
  </div>
  <div class='kpi amber'>
    <div class='lbl'>Open High</div>
    <div class='val'>{open_high}</div>
    <div class='sub'>1.5–5% unstable</div>
  </div>
  <div class='kpi {"red" if open_total else "green"}'>
    <div class='lbl'>Total Open</div>
    <div class='val'>{open_total}</div>
    <div class='sub'>Active alerts</div>
  </div>
  <div class='kpi green'>
    <div class='lbl'>Resolved</div>
    <div class='val'>{resolved}</div>
    <div class='sub'>Auto-closed</div>
  </div>
  <div class='kpi blue'>
    <div class='lbl'>Fact Rows</div>
    <div class='val'>{total_facts:,}</div>
    <div class='sub'>All parsed records</div>
  </div>
  <div class='kpi purple'>
    <div class='lbl'>Tables Monitored</div>
    <div class='val'>{unique_tbls}</div>
    <div class='sub'>In view period</div>
  </div>
</div>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════════
tab1, tab2, tab3 = st.tabs([
    "🏠 Overview",
    "🚨 Alert History",
    "🔴 Breach Visibility",
    # "📈 Historical Validation",
])


# ══════════════════════════════════════════════
# TAB 1 — Overview
# ══════════════════════════════════════════════
with tab1:
    # Current state per table (latest run per table)
    if not fdf.empty:
        latest = (fdf.sort_values('email_received_ts')
                    .groupby('table_name').last().reset_index())

        # Add previous pct for trend
        prev = (fdf.sort_values('email_received_ts')
                   .groupby('table_name').nth(-2).reset_index()[['table_name','pct_diff']]
                   .rename(columns={'pct_diff':'prev_pct_diff'}))
        latest = latest.merge(prev, on='table_name', how='left')

        def trend(row):
            if pd.isna(row.get('prev_pct_diff')): return '→'
            delta = abs(row['pct_diff']) - abs(row['prev_pct_diff'])
            if delta > 0.01:  return '↑'
            if delta < -0.01: return '↓'
            return '→'

        latest['trend'] = latest.apply(trend, axis=1)

        # Filtered to only tables with alerts
        alerted_latest = latest[latest['alert_level'].isin(['CRITICAL','HIGH'])].copy()

        st.markdown("<div class='sec-head'>Current Alert State — Tables with Active Issues</div>", unsafe_allow_html=True)

        if alerted_latest.empty:
            st.success("✅ No tables currently breaching thresholds in view period.")
        else:
            # Build styled display table
            disp = alerted_latest[['table_name','job','source_count','target_count','difference_count',
                                    'pct_diff','prev_pct_diff',
                                    'alert_level','trend']].copy()

            def fmt_level(l):
                if l == 'CRITICAL': return '🔴 CRITICAL'
                if l == 'HIGH':     return '🟡 HIGH'
                return '🟢 NORMAL'

            def fmt_trend(t):
                if t == '↑': return '↑ unstable'
                if t == '↓': return '↓ Improving'
                return '→ Stable'

            disp['pct_diff']      = disp['pct_diff'].map(lambda x: f"{x:+.3f}%")
            disp['prev_pct_diff'] = disp['prev_pct_diff'].map(lambda x: f"{x:+.3f}%" if pd.notna(x) else "—")
            disp['source_count']  = disp['source_count'].map(lambda x: f"{int(x):,}")
            disp['target_count']  = disp['target_count'].map(lambda x: f"{int(x):,}")
            disp['difference_count'] = disp['difference_count'].map(lambda x: f"{int(x):+,}")
            disp['alert_level']   = disp['alert_level'].map(fmt_level)
            disp['trend']         = disp['trend'].map(fmt_trend)
            disp.columns          = ['Table Name','Job','Source Count','Target Count','Difference',
                                      'Diff %','Prev Diff %','Alert Level','Trend']

            st.dataframe(disp, width="stretch", height=380)

        st.markdown("<div class='sec-head'>Summary by Job — Fact Table View Period</div>", unsafe_allow_html=True)

        if HAS_PX:
            job_summary = fdf.groupby('job').agg(
                total_rows=('table_name','count'),
                critical=('alert_level', lambda x: (x=='CRITICAL').sum()),
                high=('alert_level',     lambda x: (x=='HIGH').sum()),
                normal=('alert_level',   lambda x: (x=='NORMAL').sum()),
            ).reset_index().sort_values('critical', ascending=False)

            c1, c2 = st.columns([2,1])
            with c1:
                fig = go.Figure()
                fig.add_trace(go.Bar(name='CRITICAL', x=job_summary['job'], y=job_summary['critical'],
                                     marker_color='#FF3B5C', opacity=0.9))
                fig.add_trace(go.Bar(name='HIGH',     x=job_summary['job'], y=job_summary['high'],
                                     marker_color='#FFB020', opacity=0.9))
                fig.add_trace(go.Bar(name='NORMAL',   x=job_summary['job'], y=job_summary['normal'],
                                     marker_color='#1E2D4A', opacity=0.9))
                fig.update_layout(
                    barmode='stack', height=280,
                    paper_bgcolor='#0E1320', plot_bgcolor='#0E1320',
                    font=dict(color='#E2EAF8', family='JetBrains Mono', size=10),
                    margin=dict(l=10,r=10,t=10,b=10),
                    xaxis=dict(gridcolor='#1E2D4A'),
                    yaxis=dict(gridcolor='#1E2D4A'),
                    legend=dict(orientation='h', y=1.1, font=dict(size=10)),
                    showlegend=True
                )
                st.plotly_chart(fig, width="stretch")

            with c2:
                st.dataframe(job_summary, width="stretch", height=280)

        # Daily breach trend
        if HAS_PX:
            st.markdown("<div class='sec-head'>Daily Breach Trend</div>", unsafe_allow_html=True)
            fdf['date'] = fdf['email_received_ts'].dt.date
            daily = fdf.groupby('date').agg(
                critical=('alert_level', lambda x: (x=='CRITICAL').sum()),
                high=('alert_level',     lambda x: (x=='HIGH').sum()),
            ).reset_index()

            fig2 = go.Figure()
            fig2.add_trace(go.Bar(x=daily['date'], y=daily['critical'], name='CRITICAL',
                                  marker_color='#FF3B5C', opacity=0.85))
            fig2.add_trace(go.Bar(x=daily['date'], y=daily['high'], name='HIGH',
                                  marker_color='#FFB020', opacity=0.85))
            fig2.update_layout(
                barmode='group', height=220,
                paper_bgcolor='#0E1320', plot_bgcolor='#0E1320',
                font=dict(color='#E2EAF8', family='JetBrains Mono', size=10),
                margin=dict(l=10,r=10,t=10,b=10),
                xaxis=dict(gridcolor='#1E2D4A'),
                yaxis=dict(gridcolor='#1E2D4A'),
                legend=dict(orientation='h', y=1.1),
            )
            st.plotly_chart(fig2, width="stretch")


# ══════════════════════════════════════════════
# TAB 2 — Alert History
# ══════════════════════════════════════════════
with tab2:
    st.markdown("<div class='sec-head'>Alert History Table</div>", unsafe_allow_html=True)

    if alert_df.empty:
        st.info("No alerts captured yet.")
    else:
        c1, c2, c3 = st.columns(3)
        lvl_filter    = c1.multiselect("Alert Level", ['CRITICAL','HIGH'], default=['CRITICAL','HIGH'])
        status_filter = c2.multiselect("Status", ['Open','Resolved'], default=['Open','Resolved'])
        tbl_search    = c3.text_input("Search Table", "")

        adf = alert_df.copy()
        if lvl_filter:    adf = adf[adf['alert_level'].isin(lvl_filter)]
        if status_filter: adf = adf[adf['status'].isin(status_filter)]
        if tbl_search:    adf = adf[adf['table_name'].str.contains(tbl_search, case=False, na=False)]

        # Format for display
        adisp = adf[[
            'table_name','alert_level','status','difference_count',
            'pct_diff','previous_difference_count','previous_pct_diff',
            'first_seen_ts','last_seen_ts','reason','job'
        ]].copy()
        adisp['difference_count'] = adisp['difference_count'].map(lambda x: f"{int(x):+,}" if pd.notna(x) else "—")
        adisp['pct_diff']      = adisp['pct_diff'].map(lambda x: f"{float(x):+.3f}%" if pd.notna(x) else "—")
        adisp['previous_pct_diff'] = adisp['previous_pct_diff'].map(lambda x: f"{float(x):+.3f}%" if pd.notna(x) else "—")
        adisp['previous_difference_count'] = adisp['previous_difference_count'].map(lambda x: f"{int(x):+,}" if pd.notna(x) else "—")
        adisp['first_seen_ts'] = adisp['first_seen_ts'].map(lambda x: str(x)[:19] if x else "—")
        adisp['last_seen_ts']  = adisp['last_seen_ts'].map(lambda x: str(x)[:19] if x else "—")
        adisp.columns = [
            'Table Name','Level','Status','Gap','% Diff',
            'Prev Gap','Prev %','First Seen','Last Seen','Reason','Job'
        ]

        st.dataframe(adisp, width="stretch", height=460)

        # Counts
        col1, col2, col3 = st.columns(3)
        col1.metric("Open Critical", int((adf['alert_level']=='CRITICAL').sum() if 'Open' in (status_filter or ['Open']) else 0))
        col2.metric("Open HIGH",     int((adf['alert_level']=='HIGH').sum() if 'Open' in (status_filter or ['Open']) else 0))
        col3.metric("Resolved",      int((alert_df['status']=='Resolved').sum()))


# ══════════════════════════════════════════════
# TAB 3 — Breach Visibility (±5% red table)
# ══════════════════════════════════════════════
with tab3:
    st.markdown("<div class='sec-head'>±5% CRITICAL Breach Table</div>", unsafe_allow_html=True)

    crit_df = fdf[fdf['alert_level'] == 'CRITICAL'].copy()

    if crit_df.empty:
        st.success("✅ No CRITICAL breaches (≥5%) in the selected view period.")
    else:
        # Latest per table for the critical breach view
        crit_latest = (crit_df.sort_values('email_received_ts')
                               .groupby('table_name').last().reset_index())

        # Previous pct — use as_index=False so table_name stays a column, then merge
        prev_pct_map = (fdf.sort_values('email_received_ts')
                           .groupby('table_name', as_index=False)
                           .nth(-2)[['table_name', 'pct_diff']]
                           .rename(columns={'pct_diff': 'prev_pct_diff'}))
        crit_latest = crit_latest.merge(prev_pct_map, on='table_name', how='left')

        crit_disp = crit_latest[['table_name','job','pct_diff','prev_pct_diff',
                                   'source_count','target_count','difference_count',
                                   'email_received_ts']].copy()

        crit_disp['email_received_ts'] = crit_disp['email_received_ts'].dt.strftime('%Y-%m-%d %H:%M')
        crit_disp['difference_count']  = crit_disp['difference_count'].map(lambda x: f"{int(x):+,}")
        crit_disp['pct_diff']          = crit_disp['pct_diff'].map(lambda x: f"{x:+.3f}%")
        crit_disp['prev_pct_diff']     = crit_disp['prev_pct_diff'].map(lambda x: f"{x:+.3f}%" if pd.notna(x) else "—")
        crit_disp['source_count']      = crit_disp['source_count'].map(lambda x: f"{int(x):,}")
        crit_disp['target_count']      = crit_disp['target_count'].map(lambda x: f"{int(x):,}")
        crit_disp.columns = ['Table Name','Job','Diff %','Prev Diff %',
                               'Gap','Source Count','Target Count','Last Seen']

        st.dataframe(crit_disp, width="stretch", height=340)
        st.markdown(
            f"<small style='font-family:JetBrains Mono,monospace;color:#FF3B5C'>"
            f"🔴 {len(crit_latest)} tables with CRITICAL breaches (≥5%)</small>",
            unsafe_allow_html=True
        )

    st.markdown("<div class='sec-head'>1.5% – 5% HIGH Threshold Breaches</div>", unsafe_allow_html=True)

    high_df = fdf[fdf['alert_level'] == 'HIGH'].copy()
    if high_df.empty:
        st.info("No HIGH threshold breaches in view period.")
    else:
        high_latest = (high_df.sort_values('email_received_ts')
                               .groupby('table_name').last().reset_index())

        prev_pct_map2 = (fdf.sort_values('email_received_ts')
                            .groupby('table_name', as_index=False)
                            .nth(-2)[['table_name', 'pct_diff']]
                            .rename(columns={'pct_diff': 'prev_pct_diff'}))
        high_latest = high_latest.merge(prev_pct_map2, on='table_name', how='left')

        high_disp = high_latest[['table_name','job','pct_diff','prev_pct_diff',
                                   'difference_count','email_received_ts']].copy()
        high_disp['email_received_ts'] = pd.to_datetime(high_disp['email_received_ts']).dt.strftime('%Y-%m-%d %H:%M')
        high_disp['difference_count']  = high_disp['difference_count'].map(lambda x: f"{int(x):+,}")
        high_disp['pct_diff']          = high_disp['pct_diff'].map(lambda x: f"{x:+.3f}%")
        high_disp['prev_pct_diff']     = high_disp['prev_pct_diff'].map(lambda x: f"{x:+.3f}%" if pd.notna(x) else "—")
        high_disp.columns = ['Table Name','Job','Gap','Diff %','Prev Diff %','Last Seen']

        st.dataframe(high_disp, width="stretch", height=240)

    # Scatter: all breaches
    if HAS_PX and not fdf.empty:
        st.markdown("<div class='sec-head'>All Discrepancy Scatter — View Period</div>", unsafe_allow_html=True)
        scatter_df = fdf[fdf['alert_level'] != 'NORMAL'].copy()
        if not scatter_df.empty:
            color_map = {'CRITICAL':'#FF3B5C','HIGH':'#FFB020','NORMAL':'#1E2D4A'}
            fig3 = go.Figure()
            for lvl, col in color_map.items():
                s = scatter_df[scatter_df['alert_level']==lvl]
                if s.empty: continue
                fig3.add_trace(go.Scatter(
                    x=s['email_received_ts'], y=s['pct_diff'],
                    mode='markers', name=lvl,
                    marker=dict(color=col, size=8, opacity=0.8),
                    text=s['table_name'],
                    hovertemplate='<b>%{text}</b><br>%{y:+.3f}%<br>%{x}<extra></extra>'
                ))
            fig3.add_hline(y=5,   line_dash='dash', line_color='#FF3B5C', line_width=1,
                           annotation_text='+5%', annotation_font_color='#FF3B5C')
            fig3.add_hline(y=-5,  line_dash='dash', line_color='#FF3B5C', line_width=1,
                           annotation_text='-5%', annotation_font_color='#FF3B5C')
            fig3.add_hline(y=1.5, line_dash='dot',  line_color='#FFB020', line_width=1)
            fig3.add_hline(y=-1.5,line_dash='dot',  line_color='#FFB020', line_width=1)
            fig3.update_layout(
                height=300, paper_bgcolor='#0E1320', plot_bgcolor='#0E1320',
                font=dict(color='#E2EAF8', family='JetBrains Mono', size=10),
                margin=dict(l=10,r=10,t=10,b=10),
                xaxis=dict(gridcolor='#1E2D4A'),
                yaxis=dict(gridcolor='#1E2D4A', title='% Diff'),
                legend=dict(orientation='h', y=1.1),
            )
            st.plotly_chart(fig3, width="stretch")


# ══════════════════════════════════════════════
# TAB 4 — Historical Validation
# ══════════════════════════════════════════════
# with tab4:
#     st.markdown("<div class='sec-head'>Historical % Diff — Table-Level Drill-Down</div>", unsafe_allow_html=True)

#     all_tables = sorted(fact_df['table_name'].dropna().unique().tolist())
#     sel_table  = st.selectbox("Select Table", all_tables, key='hist_table')

#     if sel_table:
#         tbl_history = fact_df[fact_df['table_name'] == sel_table].copy()
#         tbl_history = tbl_history.sort_values('email_received_ts')

#         if tbl_history.empty:
#             st.warning("No history found for this table.")
#         else:
#             # KPI row for selected table
#             latest_row  = tbl_history.iloc[-1]
#             earliest_row= tbl_history.iloc[0]
#             kc1, kc2, kc3, kc4 = st.columns(4)
#             kc1.metric("Latest pct_diff",    f"{latest_row['pct_diff']:+.3f}%")
#             kc2.metric("Latest diff count",  f"{int(latest_row['difference_count']):+,}")
#             kc3.metric("Data points",         str(len(tbl_history)))
#             kc4.metric("Date range",          f"{tbl_history['run_date'].min()} → {tbl_history['run_date'].max()}")

#             if HAS_PX:
#                 fig4 = go.Figure()

#                 # Background bands
#                 fig4.add_hrect(y0=-1.5, y1=1.5, fillcolor='#00FF9D', opacity=0.04, line_width=0)
#                 fig4.add_hrect(y0=-5,   y1=-1.5, fillcolor='#FFB020', opacity=0.06, line_width=0)
#                 fig4.add_hrect(y0=1.5,  y1=5,    fillcolor='#FFB020', opacity=0.06, line_width=0)

#                 # Line + markers
#                 fig4.add_trace(go.Scatter(
#                     x=tbl_history['email_received_ts'],
#                     y=tbl_history['pct_diff'],
#                     mode='lines+markers',
#                     line=dict(color='#00D4FF', width=2),
#                     marker=dict(
#                         color=['#FF3B5C' if abs(p) >= 5 else '#FFB020' if abs(p) >= 1.5 else '#00FF9D'
#                                for p in tbl_history['pct_diff']],
#                         size=8, line=dict(color='#0E1320', width=1)
#                     ),
#                     customdata=tbl_history[['difference_count','source_count','target_count']].values,
#                     hovertemplate=(
#                         '<b>' + sel_table + '</b><br>'
#                         'Date: %{x}<br>'
#                         'Diff %: %{y:+.3f}%<br>'
#                         'Gap: %{customdata[0]:+,}<br>'
#                         'Source: %{customdata[1]:,}<br>'
#                         'Target: %{customdata[2]:,}<extra></extra>'
#                     ),
#                     name='pct_diff'
#                 ))

#                 # Threshold lines
#                 for y, col, lbl in [(5,'#FF3B5C','+5%'), (-5,'#FF3B5C','-5%'),
#                                      (1.5,'#FFB020','+1.5%'), (-1.5,'#FFB020','-1.5%')]:
#                     fig4.add_hline(
#                         y=y, line_dash='dash' if abs(y) >= 5 else 'dot',
#                         line_color=col, line_width=1,
#                         annotation_text=lbl,
#                         annotation_font_color=col,
#                         annotation_position='right'
#                     )

#                 fig4.update_layout(
#                     height=340, paper_bgcolor='#0E1320', plot_bgcolor='#0E1320',
#                     font=dict(color='#E2EAF8', family='JetBrains Mono', size=10),
#                     margin=dict(l=10, r=60, t=10, b=10),
#                     xaxis=dict(gridcolor='#1E2D4A', title='Received Timestamp'),
#                     yaxis=dict(gridcolor='#1E2D4A', title='% Difference'),
#                     showlegend=False
#                 )
#                 st.plotly_chart(fig4, width="stretch")

#                 # Source vs Target volume
#                 st.markdown("<div class='sec-head'>Source vs Target Count Over Time</div>", unsafe_allow_html=True)
#                 fig5 = go.Figure()
#                 fig5.add_trace(go.Scatter(
#                     x=tbl_history['email_received_ts'], y=tbl_history['source_count'],
#                     name='Source', line=dict(color='#00D4FF', width=2), mode='lines+markers',
#                     marker=dict(size=5)
#                 ))
#                 fig5.add_trace(go.Scatter(
#                     x=tbl_history['email_received_ts'], y=tbl_history['target_count'],
#                     name='Target', line=dict(color='#A855F7', width=2), mode='lines+markers',
#                     marker=dict(size=5)
#                 ))
#                 fig5.update_layout(
#                     height=240, paper_bgcolor='#0E1320', plot_bgcolor='#0E1320',
#                     font=dict(color='#E2EAF8', family='JetBrains Mono', size=10),
#                     margin=dict(l=10,r=10,t=10,b=10),
#                     xaxis=dict(gridcolor='#1E2D4A'),
#                     yaxis=dict(gridcolor='#1E2D4A', title='Row Count'),
#                     legend=dict(orientation='h', y=1.1),
#                 )
#                 st.plotly_chart(fig5, width="stretch")

            # Raw history table
            # st.markdown("<div class='sec-head'>Raw History Rows</div>", unsafe_allow_html=True)
            # raw_disp = tbl_history[['email_received_ts','job','source_count',
            #                          'target_count','difference_count','pct_diff','alert_level']].copy()
            # raw_disp['email_received_ts'] = pd.to_datetime(raw_disp['email_received_ts']).dt.strftime('%Y-%m-%d %H:%M')
            # raw_disp['pct_diff']          = raw_disp['pct_diff'].map(lambda x: f"{x:+.4f}%")
            # raw_disp['source_count']      = raw_disp['source_count'].map(lambda x: f"{int(x):,}")
            # raw_disp['target_count']      = raw_disp['target_count'].map(lambda x: f"{int(x):,}")
            # raw_disp['difference_count']  = raw_disp['difference_count'].map(lambda x: f"{int(x):+,}")
            # raw_disp.columns = ['Received','Job','Source','Target','Gap','% Diff','Alert Level']
            # st.dataframe(raw_disp, width="stretch", height=320)

