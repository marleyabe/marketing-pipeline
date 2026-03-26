from datetime import date

import psycopg2.extensions

# Thresholds globais
SPEND_WARNING_PCT = -30
SPEND_CRITICAL_PCT = -50
CONVERSION_WARNING_PCT = -50
CONVERSION_CRITICAL_PCT = -70


def _severity(change_pct: float, warning_threshold: float, critical_threshold: float) -> str:
    if change_pct <= critical_threshold:
        return "critical"
    if change_pct <= warning_threshold:
        return "warning"
    return ""


class AlertDetector:
    def __init__(self, connection: psycopg2.extensions.connection):
        self._conn = connection

    def detect(self, check_date: date) -> list[dict]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT account_id, account_name, date, spend, prev_spend, "
                "conversions, prev_conversions, spend_change_pct, conversion_change_pct "
                "FROM gold.alerts_daily WHERE date = %s",
                [check_date],
            )
            rows = cur.fetchall()

        alerts = []
        for row in rows:
            account_id, account_name = row[0], row[1]
            row_date = row[2]
            spend_change = row[7] or 0.0
            conv_change = row[8] or 0.0

            # Check spend drop
            sev = _severity(spend_change, SPEND_WARNING_PCT, SPEND_CRITICAL_PCT)
            if sev:
                alert = {
                    "account_id": account_id,
                    "account_name": account_name,
                    "date": row_date,
                    "alert_type": "daily",
                    "metric_name": "spend",
                    "current_value": row[3],
                    "previous_value": row[4],
                    "change_pct": spend_change,
                    "severity": sev,
                }
                alerts.append(alert)
                with self._conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO gold.active_alerts "
                        "(account_id, account_name, date, alert_type, metric_name, "
                        "current_value, previous_value, change_pct, severity) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        [account_id, account_name, row_date, "daily", "spend",
                         row[3], row[4], spend_change, sev],
                    )
                self._conn.commit()

            # Check conversion drop
            sev = _severity(conv_change, CONVERSION_WARNING_PCT, CONVERSION_CRITICAL_PCT)
            if sev:
                alert = {
                    "account_id": account_id,
                    "account_name": account_name,
                    "date": row_date,
                    "alert_type": "daily",
                    "metric_name": "conversions",
                    "current_value": row[5],
                    "previous_value": row[6],
                    "change_pct": conv_change,
                    "severity": sev,
                }
                alerts.append(alert)
                with self._conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO gold.active_alerts "
                        "(account_id, account_name, date, alert_type, metric_name, "
                        "current_value, previous_value, change_pct, severity) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        [account_id, account_name, row_date, "daily", "conversions",
                         row[5], row[6], conv_change, sev],
                    )
                self._conn.commit()

        return alerts
