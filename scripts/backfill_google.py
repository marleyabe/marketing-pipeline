"""Runner manual do backfill Google Ads (fora do Airflow).

Por que existe:
    Quando só o postgres está de pé (sem scheduler/webserver), não dá pra
    usar `airflow dags trigger google_ads --conf '{...}'`. Subir o compose
    inteiro só para um backfill one-off custa build + init + webserver —
    vários minutos de overhead.

    Este script chama exatamente a mesma pipeline resiliente de
    dags._extractor (commit-por-partição + retry transiente), apenas
    fornecendo um `context` mock com os params de data, sem depender do
    Airflow. Dá pra rodar em container one-off na network do postgres.

Exemplo:
    python scripts/backfill_google.py 2026-04-01 2026-04-23
"""

import logging
import sys

from dags._extractor import run_extraction
from dags.google_ads import KEYWORDS_SPEC, NEGATIVES_SPEC, SEARCH_TERMS_SPEC
from dags._extractor import run_snapshot_extraction


def _parse_args(argv: list[str]) -> tuple[str, str]:
    if len(argv) != 3:
        raise SystemExit("usage: backfill_google.py START_DATE END_DATE (YYYY-MM-DD)")
    return argv[1], argv[2]


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    start, end = _parse_args(sys.argv)
    context = {"params": {"start_date": start, "end_date": end}}

    kw = run_extraction(KEYWORDS_SPEC, context)
    print(f"keywords: {kw} linhas carregadas")

    st = run_extraction(SEARCH_TERMS_SPEC, context)
    print(f"search_terms: {st} linhas carregadas")

    neg = run_snapshot_extraction(NEGATIVES_SPEC)
    print(f"negatives (snapshot hoje): {neg} linhas carregadas")


if __name__ == "__main__":
    main()
