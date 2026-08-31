import json
import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from api_client import fetch_paginated_data
from config import get_bronze_path


if len(sys.argv) < 2:
    print("Uso: python ingest_constructor_standings.py <season>")
    sys.exit(1)


SEASON = sys.argv[1]

url = (
    f"https://api.jolpi.ca/ergast/f1/"
    f"{SEASON}/constructorstandings/"
)


standings_lists = fetch_paginated_data(
    url=url,
    table_key="StandingsTable",
    records_key="StandingsLists"
)


final_data = {
    "season": SEASON,
    "total": len(standings_lists),
    "standings": standings_lists
}


output_path = get_bronze_path(
    SEASON,
    "constructor_standings"
)


output_path.parent.mkdir(
    parents=True,
    exist_ok=True
)


with open(
    output_path,
    "w",
    encoding="utf-8"
) as file:

    json.dump(
        final_data,
        file,
        ensure_ascii=False,
        indent=4
    )


print()
print(
    f"Total salvo: {len(standings_lists)} "
    f"listas de classificação"
)
print(f"Arquivo salvo em: {output_path}")