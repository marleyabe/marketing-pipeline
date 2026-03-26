from pathlib import Path

import psycopg2.extensions


class SQLRunner:
    def __init__(self, connection: psycopg2.extensions.connection, sql_dir: str):
        self._conn = connection
        self._sql_dir = Path(sql_dir)

    def _run_folder(self, folder: str) -> None:
        """Execute all .sql files in a folder in alphabetical order."""
        folder_path = self._sql_dir / folder
        if not folder_path.exists():
            return

        sql_files = sorted(folder_path.glob("*.sql"))
        for sql_file in sql_files:
            sql = sql_file.read_text()
            with self._conn.cursor() as cur:
                cur.execute(sql)
            self._conn.commit()

    def run_silver(self) -> None:
        self._run_folder("silver")

    def run_gold(self) -> None:
        self._run_folder("gold")

    def run_all(self) -> None:
        self.run_silver()
        self.run_gold()
