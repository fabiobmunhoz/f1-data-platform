import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from api_client import fetch_paginated_data
from config import get_bronze_path

import requests


if len(sys.argv) < 2:
    print("Uso: python <script>.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

LIMIT = 100

url = f"https://api.jolpi.ca/ergast/f1/{SEASON}/results/"

all_results = []

offset = 0

while True:
    params = {
        "limit": LIMIT,
        "offset": offset
    }

    response = requests.get(
        url,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    data = response.json()

    mrdata = data["MRData"]
    races = mrdata["RaceTable"]["Races"]

    page_results = []

    for race in races:
        for result in race.get("Results", []):
            record = {
                "season": race["season"],
                "round": race["round"],
                "raceName": race["raceName"],
                "date": race["date"],
                "circuitId": race["Circuit"]["circuitId"],
                "result": result
            }

            page_results.append(record)

    all_results.extend(page_results)

    total = int(mrdata["total"])

    print(
        f"Offset {offset}: "
        f"{len(page_results)} resultados recebidos"
    )

    if len(all_results) >= total:
        break

    if len(page_results) == 0:
        break

    offset += LIMIT


final_data = {
    "season": SEASON,
    "total": len(all_results),
    "results": all_results
}


output_path = get_bronze_path(
    SEASON,
    "results"
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
print(f"Total salvo: {len(all_results)} resultados")
print(f"Arquivo salvo em: {output_path}")