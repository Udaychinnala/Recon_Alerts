"""
alert_engine.py — Alert classification with unstable-only logic

Rules:
  CRITICAL : abs(pct_diff) >= 5.0              → always capture
  HIGH     : 1.5 <= abs(pct_diff) < 5.0        → only if first occurrence OR unstable
  NORMAL   : abs(pct_diff) < 1.5               → auto-resolve open alerts
"""
from database import get_last_alert, upsert_alert, auto_resolve_alerts

CRITICAL_THRESHOLD = 5.0
HIGH_THRESHOLD     = 1.5


def classify_and_capture(records: list[dict]) -> dict:
    """
    Process a batch of fact records through the alert engine.
    Returns summary counts.
    """
    stats = {'critical': 0, 'high': 0, 'normal': 0, 'ignored': 0}

    for rec in records:
        pct       = abs(rec['pct_diff'])
        curr_gap  = abs(rec['difference_count'])
        tbl       = rec['table_name']
        ts        = str(rec.get('email_received_ts', ''))
        run_id    = rec['run_id']
        job       = rec.get('job', '')

        if pct >= CRITICAL_THRESHOLD:
            # Always capture CRITICAL
            prev = get_last_alert(tbl)
            prev_diff = prev['difference_count'] if prev else None
            prev_pct  = prev['pct_diff'] if prev else None
            upsert_alert(tbl, run_id, 'CRITICAL',
                         rec['difference_count'], rec['pct_diff'],
                         prev_diff, prev_pct,
                         f"≥{CRITICAL_THRESHOLD}% breach ({rec['pct_diff']:+.3f}%)",
                         job, ts)
            rec['alert_level'] = 'CRITICAL'
            stats['critical'] += 1

        elif pct >= HIGH_THRESHOLD:
            prev = get_last_alert(tbl)

            if prev is None:
                # First occurrence — capture
                upsert_alert(tbl, run_id, 'HIGH',
                             rec['difference_count'], rec['pct_diff'],
                             None, None,
                             f"≥{HIGH_THRESHOLD}% first occurrence ({rec['pct_diff']:+.3f}%)",
                             job, ts)
                rec['alert_level'] = 'HIGH'
                stats['high'] += 1
            else:
                prev_gap = abs(prev['difference_count']) if prev['difference_count'] is not None else 0
                if curr_gap > prev_gap:
                    # unstable — capture
                    upsert_alert(tbl, run_id, 'HIGH',
                                 rec['difference_count'], rec['pct_diff'],
                                 prev['difference_count'], prev['pct_diff'],
                                 f"≥{HIGH_THRESHOLD}% unstable: gap {prev_gap}→{curr_gap} ({rec['pct_diff']:+.3f}%)",
                                 job, ts)
                    rec['alert_level'] = 'HIGH'
                    stats['high'] += 1
                else:
                    # Breach but not unstable — ignore
                    rec['alert_level'] = 'HIGH'   # still mark in fact table
                    stats['ignored'] += 1
        else:
            rec['alert_level'] = 'NORMAL'
            stats['normal'] += 1

    # Auto-resolve alerts whose latest run shows pct < 1.5%
    auto_resolve_alerts()

    return stats
