import json
import sys
from pathlib import Path

import pandas as pd


if len(sys.argv) < 2:
    print("Uso: python transform_races.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

input_path = Path(
    f"data/bronze/season={SEASON}/races.json"
)

output_path = Path(
    f"data/silver/season={SEASON}/races.parquet"
)

with open(
    input_path,
    "r",
    encoding="utf-8"
) as file:
    data = json.load(file)

races = data["races"]

records = []

for race in races:
    circuit = race.get("Circuit", {})
    location = circuit.get("Location", {})

    record = {
        "season": race.get("season"),
        "round": race.get("round"),
        "race_name": race.get("raceName"),
        "date": race.get("date"),
        "circuit_id": circuit.get("circuitId"),
        "circuit_name": circuit.get("circuitName"),
        "locality": location.get("locality"),
        "country": location.get("country"),
        "latitude": location.get("lat"),
        "longitude": location.get("long")
    }

    records.append(record)

df = pd.DataFrame(records)

df["season"] = pd.to_numeric(
    df["season"],
    errors="coerce"
)

df["round"] = pd.to_numeric(
    df["round"],
    errors="coerce"
)

df["date"] = pd.to_datetime(
    df["date"],
    errors="coerce"
)

df["latitude"] = pd.to_numeric(
    df["latitude"],
    errors="coerce"
)

df["longitude"] = pd.to_numeric(
    df["longitude"],
    errors="coerce"
)

df = df.drop_duplicates(
    subset=["season", "round"]
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
print(f"Registros Bronze: {len(races)}")
print(f"Registros Silver: {len(df)}")
print(f"Arquivo salvo em: {output_path}")