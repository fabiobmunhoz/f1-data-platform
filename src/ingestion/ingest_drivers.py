import json
from pathlib import Path

import requests


SEASON = 2025
LIMIT = 30

url = f"https://api.jolpi.ca/ergast/f1/{SEASON}/drivers/"

all_drivers = []

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
    drivers = mrdata["DriverTable"]["Drivers"]

    all_drivers.extend(drivers)

    total = int(mrdata["total"])

    print(
        f"Offset {offset}: "
        f"{len(drivers)} pilotos recebidos"
    )

    if len(all_drivers) >= total:
        break

    offset += LIMIT


final_data = {
    "season": SEASON,
    "total": len(all_drivers),
    "drivers": all_drivers
}


output_path = Path(
    f"data/bronze/season={SEASON}/drivers.json"
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
print(f"Total salvo: {len(all_drivers)} pilotos")
print(f"Arquivo salvo em: {output_path}")