"""Gera API key, armazena hash no Postgres, imprime raw.

Uso:
  python scripts/create_api_key.py --name <nome> [--user-id <id>]
"""

import argparse

from src.api.auth import generate_api_key, hash_api_key
from src.db import get_pg, init_schemas


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--user-id", type=int, default=None)
    arguments = parser.parse_args()

    connection = get_pg()
    init_schemas(connection)
    raw_key = generate_api_key()
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO ops.api_keys (name, key_hash, user_id) VALUES (%s, %s, %s)",
            [arguments.name, hash_api_key(raw_key), arguments.user_id],
        )
    connection.close()
    print(f"API key '{arguments.name}': {raw_key}")
    print("Guarde. Não será mostrada novamente.")


if __name__ == "__main__":
    main()
