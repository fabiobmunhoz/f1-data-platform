import json
import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

import requests

from config import get_bronze_path


if len(sys.argv) < 2:
    print("Uso: python ingest_sprint_results.py <season>")
    sys.exit(1)


SEASON = sys.argv[1]

url = (
    f"https://api.jolpi.ca/ergast/f1/"
    f"{SEASON}/sprint/"
)


response = requests.get(
    url,
    params={
        "limit": 1000
    },
    timeout=30
)

response.raise_for_status()

data = response.json()

races = (
    data["MRData"]
    ["RaceTable"]
    ["Races"]
)


final_data = {
    "season": SEASON,
    "total": len(races),
    "races": races
}


output_path = get_bronze_path(
    SEASON,
    "sprint_results"
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
    f"Total salvo: {len(races)} corridas com Sprint"
)
print(
    f"Arquivo salvo em: {output_path}"
)