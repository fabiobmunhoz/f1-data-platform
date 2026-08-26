import sys
from pathlib import Path

import pandas as pd


if len(sys.argv) < 2:
    print("Uso: python check_gold_results.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

input_path = Path(
    f"data/gold/season={SEASON}/fact_race_results.parquet"
)

df = pd.read_parquet(input_path)


print()
print(f"Temporada: {SEASON}")
print(f"Total de registros Gold: {len(df)}")


# 1. Verificar duplicidades na granularidade da fato
duplicate_count = df.duplicated(
    subset=["season", "round", "driver_id"]
).sum()

print(
    "Duplicados season/round/driver_id:",
    duplicate_count
)

if duplicate_count > 0:
    raise ValueError(
        "Falha Gold: existem resultados duplicados."
    )


# 2. Verificar campos críticos
critical_columns = [
    "season",
    "round",
    "driver_id",
    "driver_name",
    "constructor_id",
    "constructor_name",
    "race_name"
]

for column in critical_columns:

    null_count = df[column].isnull().sum()

    print(
        f"Nulos em {column}: {null_count}"
    )

    if null_count > 0:
        raise ValueError(
            f"Falha Gold: {column} possui valores nulos."
        )


# 3. Verificar quantidade de registros
if len(df) == 0:
    raise ValueError(
        "Falha Gold: tabela não possui registros."
    )


print()
print("✅ Gold validada com sucesso.")