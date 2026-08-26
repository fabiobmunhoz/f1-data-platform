import sys
from pathlib import Path

import pandas as pd


if len(sys.argv) < 2:
    print("Uso: python check_results.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

input_path = Path(
    f"data/silver/season={SEASON}/results.parquet"
)

df = pd.read_parquet(input_path)

print()
print(f"Temporada: {SEASON}")
print(f"Total de registros: {len(df)}")

print()
print("Schema:")
print(df.dtypes)

print()
print("Valores nulos:")
print(df.isnull().sum())

duplicate_count = (
    df.duplicated(
        subset=[
            "season",
            "round",
            "driver_id"
        ]
    )
    .sum()
)

print()
print(
    "Resultados duplicados por "
    "season/round/driver_id:",
    duplicate_count
)

if duplicate_count > 0:
    raise ValueError(
        "Falha de qualidade: existem resultados duplicados."
    )

critical_columns = [
    "season",
    "round",
    "driver_id",
    "constructor_id"
]

for column in critical_columns:
    if df[column].isnull().any():
        raise ValueError(
            f"Falha de qualidade: {column} possui valores nulos."
        )

print()
print("✅ Validação concluída com sucesso.")