import sys
from pathlib import Path

import pandas as pd


if len(sys.argv) < 2:
    print("Uso: python build_gold_results.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

base_path = Path(f"data/silver/season={SEASON}")

results = pd.read_parquet(base_path / "results.parquet")
drivers = pd.read_parquet(base_path / "drivers.parquet")
constructors = pd.read_parquet(base_path / "constructors.parquet")
races = pd.read_parquet(base_path / "races.parquet")


drivers = drivers[
    [
        "driver_id",
        "given_name",
        "family_name",
        "nationality"
    ]
].copy()

drivers["driver_name"] = (
    drivers["given_name"]
    + " "
    + drivers["family_name"]
)


constructors = constructors[
    [
        "constructor_id",
        "name"
    ]
].rename(
    columns={
        "name": "constructor_name"
    }
)


races = races[
    [
        "season",
        "round",
        "race_name",
        "country",
        "circuit_name"
    ]
]


gold = results.merge(
    drivers[
        [
            "driver_id",
            "driver_name",
            "nationality"
        ]
    ],
    on="driver_id",
    how="left"
)


gold = gold.merge(
    constructors,
    on="constructor_id",
    how="left"
)


gold = gold.merge(
    races,
    on=[
        "season",
        "round"
    ],
    how="left",
    suffixes=("", "_race")
)


output_path = Path(
    f"data/gold/season={SEASON}/fact_race_results.parquet"
)

output_path.parent.mkdir(
    parents=True,
    exist_ok=True
)

gold.to_parquet(
    output_path,
    index=False
)


print()
print(f"Temporada: {SEASON}")
print(f"Resultados Silver: {len(results)}")
print(f"Registros Gold: {len(gold)}")
print(f"Arquivo salvo em: {output_path}")