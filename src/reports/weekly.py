from datetime import date, datetime

import duckdb


WEEKLY_TEMPLATE = """📊 Relatório Semanal – {account_name}
📅 Período: {period_start} a {period_end}

📢 Impressões: {impressoes}
🖱️ Cliques: {cliques}
🎯 Conversões: {conversoes}
💲 Custo por conversão: R$ {custo_por_conversao}
💰 Investimento: R$ {investimento}"""


class WeeklyReportGenerator:
    def __init__(self, connection: duckdb.DuckDBPyConnection):
        self._conn = connection

    def generate(self, report_date: date) -> list[dict]:
        rows = self._conn.execute(
            "SELECT account_id, account_name, period_start, period_end, "
            "investimento, impressoes, cliques, conversoes, custo_por_conversao "
            "FROM gold.reports_weekly WHERE week_start = DATE_TRUNC('week', ?::DATE)",
            [report_date],
        ).fetchall()

        reports = []
        for row in rows:
            report_text = WEEKLY_TEMPLATE.format(
                account_name=row[1],
                period_start=row[2].strftime("%d/%m"),
                period_end=row[3].strftime("%d/%m"),
                investimento=f"{row[4]:,.1f}".replace(",", "."),
                impressoes=row[5],
                cliques=row[6],
                conversoes=int(row[7]),
                custo_por_conversao=f"{row[8]:,.2f}".replace(",", "."),
            )
            report = {
                "account_id": row[0],
                "account_name": row[1],
                "report_type": "weekly",
                "report_date": report_date,
                "report_text": report_text,
            }
            reports.append(report)

            self._conn.execute(
                "INSERT INTO gold.generated_reports "
                "(account_id, account_name, report_type, report_date, report_text) "
                "VALUES (?, ?, ?, ?, ?)",
                [row[0], row[1], "weekly", report_date, report_text],
            )

        return reports
