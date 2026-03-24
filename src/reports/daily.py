from datetime import date, datetime

import duckdb


DAILY_TEMPLATE = """**{account_name}**
* Data: {date}
* Investimento: R${investimento}
* Impressões: {impressoes}
* Cliques: {cliques}
* Conversões: {conversoes}
* Custo por conversão: R${custo_por_conversao}
* Taxa de Conversão: {taxa_de_conversao}%"""


class DailyReportGenerator:
    def __init__(self, connection: duckdb.DuckDBPyConnection):
        self._conn = connection

    def generate(self, report_date: date) -> list[dict]:
        rows = self._conn.execute(
            "SELECT account_id, account_name, date, investimento, impressoes, "
            "cliques, conversoes, custo_por_conversao, taxa_de_conversao "
            "FROM gold.reports_daily WHERE date = ?",
            [report_date],
        ).fetchall()

        reports = []
        for row in rows:
            report_text = DAILY_TEMPLATE.format(
                account_name=row[1],
                date=row[2].strftime("%d/%m/%Y"),
                investimento=f"{row[3]:,.1f}".replace(",", "."),
                impressoes=row[4],
                cliques=row[5],
                conversoes=int(row[6]),
                custo_por_conversao=f"{row[7]:,.2f}".replace(",", "."),
                taxa_de_conversao=row[8],
            )
            report = {
                "account_id": row[0],
                "account_name": row[1],
                "report_type": "daily",
                "report_date": report_date,
                "report_text": report_text,
            }
            reports.append(report)

            self._conn.execute(
                "INSERT INTO gold.generated_reports "
                "(account_id, account_name, report_type, report_date, report_text) "
                "VALUES (?, ?, ?, ?, ?)",
                [row[0], row[1], "daily", report_date, report_text],
            )

        return reports
