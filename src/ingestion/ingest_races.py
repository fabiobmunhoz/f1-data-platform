import json
from pathlib import Path

from api_client import fetch_paginated_data


SEASON = 2025

url = f"https://api.jolpi.ca/ergast/f1/{SEASON}/races/"

races = fetch_paginated_data(
    url=url,
    table_key="RaceTable",
    records_key="Races"
)

final_data = {
    "season": SEASON,
    "total": len(races),
    "races": races
}

output_path = Path(
    f"data/bronze/season={SEASON}/races.json"
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
print(f"Total salvo: {len(races)} corridas")
print(f"Arquivo salvo em: {output_path}")