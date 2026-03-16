"""
parser.py — QDI Monitor
Parses actual HTML tables from Power Automate email bodies.
Handles all email types: odw, eservice, Proficy, i360, ADW, MDM etc.
"""
import re
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime


def _to_int(val):
    if val is None: return None
    try: return int(str(val).replace(',', '').strip())
    except: return None


def _to_float(val):
    if val is None: return None
    try: return float(str(val).replace(',', '').strip())
    except: return None


def _clean_ts(val):
    if not val or str(val).lower() in ('nan', 'none', ''): return None
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S%z',
                '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try: return datetime.strptime(str(val).strip()[:19], fmt)
        except: continue
    return None


def _infer_job(subject: str) -> str:
    s = subject.lower()
    if 'proficy_grr' in s: return 'Proficy_GRR_QDI2Hive'
    if 'proficy' in s and 'rds' in s: return 'Proficy_QDI2RDS'
    if 'proficy' in s: return 'Proficy_QDI2Hive'
    if 'eservice' in s: return 'eservice_QDI2Hive'
    if 'i360' in s: return 'i360_mirror'
    if 'adw' in s: return 'ADW_tables'
    if 'mdm' in s: return 'MDM_S3_to_RDS'
    if 'odw' in s: return 'ODW_QDI2Hive'
    return re.sub(r'\s+', '_', subject.strip()[:40])


def _find_col(headers, keywords):
    for i, h in enumerate(headers):
        if any(k in h for k in keywords):
            return i
    return None


def parse_html_to_records(raw_html: str, subject: str, received_ts, email_id, run_id: int) -> list[dict]:
    """Parse one HTML email body into a list of structured row dicts."""
    soup = BeautifulSoup(str(raw_html), 'html.parser')
    table = soup.find('table')
    if not table:
        return []

    thead = table.find('thead')
    tbody = table.find('tbody')
    if not thead or not tbody:
        return []

    raw_headers = [th.get_text(strip=True) for th in thead.find_all('th')]
    headers = [h.lower().replace(' ', '_') for h in raw_headers]

    src_table_i  = _find_col(headers, ['source_table'])
    src_count_i  = _find_col(headers, ['source_count'])
    src_date_i   = _find_col(headers, ['source_last', 'source_updated'])
    tgt_table_i  = _find_col(headers, ['target_table'])
    tgt_count_i  = _find_col(headers, ['target_count'])
    tgt_date_i   = _find_col(headers, ['hive_updated', 'last_update', 'target_last'])
    diff_i       = _find_col(headers, ['difference'])

    job = _infer_job(subject)
    ts  = _clean_ts(str(received_ts))

    records = []
    for tr in tbody.find_all('tr'):
        cells = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]

        def g(i, default=None):
            if i is None or i >= len(cells): return default
            v = cells[i].strip()
            return None if v.lower() in ('nan', '', 'none') else v

        src_tbl = g(src_table_i)
        if not src_tbl or 'table' in str(src_tbl).lower()[:10]:
            continue

        src_cnt = _to_int(g(src_count_i))
        tgt_cnt = _to_int(g(tgt_count_i))
        if src_cnt is None or tgt_cnt is None:
            continue

        diff = _to_int(g(diff_i))
        if diff is None:
            diff = tgt_cnt - src_cnt

        pct = round(((tgt_cnt - src_cnt) / src_cnt) * 100, 4) if src_cnt != 0 else 0.0

        records.append({
            'run_id':              run_id,
            'email_id':            email_id,
            'email_received_ts':   ts,
            'run_date':            ts.date() if ts else None,
            'job':                 job,
            'subject':             subject,
            'table_name':          src_tbl,
            'target_table':        g(tgt_table_i),
            'source_count':        src_cnt,
            'target_count':        tgt_cnt,
            'difference_count':    diff,
            'pct_diff':            pct,
            'source_last_updated': _clean_ts(g(src_date_i)),
            'target_last_updated': _clean_ts(g(tgt_date_i)),
        })

    return records


def read_source_excel(excel_path: str, sheet_name: str = 'Sheet1') -> pd.DataFrame:
    df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype=str)
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
    return df


def parse_new_rows(excel_path: str, from_row: int, sheet_name: str = 'Sheet1'):
    """Incremental: parse only rows[from_row:]. Returns (records_list, total_row_count)."""
    df = read_source_excel(excel_path, sheet_name)
    total = len(df)
    if from_row >= total:
        return [], total

    records = []
    for i, row in df.iloc[from_row:].iterrows():
        recs = parse_html_to_records(
            row.get('raw_html', ''),
            row.get('subject', ''),
            row.get('received_ts', ''),
            row.get('email_id', ''),
            run_id=i
        )
        records.extend(recs)

    return records, total
