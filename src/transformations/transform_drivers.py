import json
import sys
from pathlib import Path

import pandas as pd


if len(sys.argv) < 2:
    print("Uso: python transform_drivers.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]


input_path = Path(
    f"data/bronze/season={SEASON}/drivers.json"
)

output_path = Path(
    f"data/silver/season={SEASON}/drivers.parquet"
)


with open(
    input_path,
    "r",
    encoding="utf-8"
) as file:
    data = json.load(file)


drivers = data["drivers"]


records = []

for driver in drivers:

    record = {
        "driver_id": driver.get("driverId"),
        "permanent_number": driver.get("permanentNumber"),
        "code": driver.get("code"),
        "given_name": driver.get("givenName"),
        "family_name": driver.get("familyName"),
        "date_of_birth": driver.get("dateOfBirth"),
        "nationality": driver.get("nationality")
    }

    records.append(record)


df = pd.DataFrame(records)


df["date_of_birth"] = pd.to_datetime(
    df["date_of_birth"],
    errors="coerce"
)


df = df.drop_duplicates(
    subset=["driver_id"]
)


output_path.parent.mkdir(
    parents=True,
    exist_ok=True
)


df.to_parquet(
    output_path,
    index=False
)


print()
print(f"Temporada: {SEASON}")
print(f"Registros Bronze: {len(drivers)}")
print(f"Registros Silver: {len(df)}")
print(f"Arquivo salvo em: {output_path}")