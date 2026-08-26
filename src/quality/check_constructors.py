import sys
from pathlib import Path

import pandas as pd


if len(sys.argv) < 2:
    print("Uso: python check_constructors.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

input_path = Path(
    f"data/silver/season={SEASON}/constructors.parquet"
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

duplicate_count = df["constructor_id"].duplicated().sum()

print()
print(f"constructor_id duplicados: {duplicate_count}")

if duplicate_count > 0:
    raise ValueError(
        "Falha de qualidade: existem constructor_id duplicados."
    )

if df["constructor_id"].isnull().any():
    raise ValueError(
        "Falha de qualidade: existem constructor_id nulos."
    )

if df["name"].isnull().any():
    raise ValueError(
        "Falha de qualidade: existem nomes de construtores nulos."
    )

print()
print("✅ Validação concluída com sucesso.")