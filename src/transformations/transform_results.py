import json
import sys
from pathlib import Path

import pandas as pd


if len(sys.argv) < 2:
    print("Uso: python transform_results.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

input_path = Path(
    f"data/bronze/season={SEASON}/results.json"
)

output_path = Path(
    f"data/silver/season={SEASON}/results.parquet"
)

with open(
    input_path,
    "r",
    encoding="utf-8"
) as file:
    data = json.load(file)

results = data["results"]

records = []

for item in results:
    result = item["result"]
    driver = result.get("Driver", {})
    constructor = result.get("Constructor", {})
    fastest_lap = result.get("FastestLap", {})

    record = {
        "season": item.get("season"),
        "round": item.get("round"),
        "race_name": item.get("raceName"),
        "race_date": item.get("date"),
        "circuit_id": item.get("circuitId"),

        "driver_id": driver.get("driverId"),
        "constructor_id": constructor.get("constructorId"),

        "grid": result.get("grid"),
        "position": result.get("position"),
        "position_text": result.get("positionText"),
        "points": result.get("points"),
        "laps": result.get("laps"),
        "status": result.get("status"),

        "fastest_lap_rank": fastest_lap.get("rank")
    }

    records.append(record)

df = pd.DataFrame(records)

numeric_columns = [
    "season",
    "round",
    "grid",
    "position",
    "points",
    "laps",
    "fastest_lap_rank"
]

for column in numeric_columns:
    df[column] = pd.to_numeric(
        df[column],
        errors="coerce"
    )

df["race_date"] = pd.to_datetime(
    df["race_date"],
    errors="coerce"
)

df = df.drop_duplicates(
    subset=[
        "season",
        "round",
        "driver_id"
    ]
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
print(f"Registros Bronze: {len(results)}")
print(f"Registros Silver: {len(df)}")
print(f"Arquivo salvo em: {output_path}")