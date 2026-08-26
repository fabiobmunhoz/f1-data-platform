import json
import sys
from pathlib import Path

import pandas as pd


if len(sys.argv) < 2:
    print("Uso: python transform_constructors.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

input_path = Path(
    f"data/bronze/season={SEASON}/constructors.json"
)

output_path = Path(
    f"data/silver/season={SEASON}/constructors.parquet"
)

with open(
    input_path,
    "r",
    encoding="utf-8"
) as file:
    data = json.load(file)

constructors = data["constructors"]

records = []

for constructor in constructors:
    record = {
        "constructor_id": constructor.get("constructorId"),
        "name": constructor.get("name"),
        "nationality": constructor.get("nationality")
    }

    records.append(record)

df = pd.DataFrame(records)

df = df.drop_duplicates(
    subset=["constructor_id"]
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
print(f"Registros Bronze: {len(constructors)}")
print(f"Registros Silver: {len(df)}")
print(f"Arquivo salvo em: {output_path}")