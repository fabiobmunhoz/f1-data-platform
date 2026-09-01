import sys
from pathlib import Path

import requests

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from config import get_bronze_path
from storage import save_json


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


save_json(
    final_data,
    output_path
)


print()
print(
    f"Total salvo: {len(races)} corridas com Sprint"
)
print(
    f"Arquivo salvo em: {output_path}"
)