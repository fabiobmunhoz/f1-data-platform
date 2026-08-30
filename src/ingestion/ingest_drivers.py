import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from api_client import fetch_paginated_data
from config import get_bronze_path


if len(sys.argv) < 2:
    print("Uso: python ingest_drivers.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

url = f"https://api.jolpi.ca/ergast/f1/{SEASON}/drivers/"

drivers = fetch_paginated_data(
    url=url,
    table_key="DriverTable",
    records_key="Drivers"
)

final_data = {
    "season": SEASON,
    "total": len(drivers),
    "drivers": drivers
}

output_path = get_bronze_path(
    SEASON,
    "drivers"
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
print(f"Total salvo: {len(drivers)} pilotos")
print(f"Arquivo salvo em: {output_path}")