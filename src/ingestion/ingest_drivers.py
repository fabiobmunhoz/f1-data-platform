import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from api_client import fetch_paginated_data
from config import get_bronze_path
from storage import save_json


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

save_json(
    final_data,
    output_path
)

print()
print(f"Total salvo: {len(drivers)} pilotos")
print(f"Arquivo salvo em: {output_path}")