import sys

import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)
from pyspark.sql.functions import col, count, when
from spark_utils import create_spark_session

if len(sys.argv) < 2:
    print("Uso: python check_gold_results_spark.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

input_path = (
    f"data/gold_spark/season={SEASON}/fact_race_results"
)

spark = create_spark_session(
    "F1GoldQualityCheck"
)

df = spark.read.parquet(input_path)

print()
print(f"Temporada: {SEASON}")
print(f"Total de registros Gold: {df.count()}")

duplicate_count = (
    df.groupBy(
        "season",
        "round",
        "driver_id"
    )
    .count()
    .filter(col("count") > 1)
    .count()
)

print(
    "Duplicados season/round/driver_id:",
    duplicate_count
)

if duplicate_count > 0:
    raise ValueError(
        "Falha Gold Spark: existem registros duplicados."
    )

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
    null_count = (
        df
        .filter(col(column).isNull())
        .count()
    )

    print(
        f"Nulos em {column}: {null_count}"
    )

    if null_count > 0:
        raise ValueError(
            f"Falha Gold Spark: {column} possui valores nulos."
        )

if df.count() == 0:
    raise ValueError(
        "Falha Gold Spark: tabela vazia."
    )

print()
print("✅ Gold Spark validada com sucesso.")

spark.stop()