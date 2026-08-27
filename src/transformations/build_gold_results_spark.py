import sys

from pyspark.sql.functions import col, concat_ws
import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from spark_utils import create_spark_session

if len(sys.argv) < 2:
    print("Uso: python build_gold_results_spark.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

base_path = f"data/silver_spark/season={SEASON}"
output_path = f"data/gold_spark/season={SEASON}/fact_race_results"


spark = create_spark_session(
    "F1GoldResults"
)


results = spark.read.parquet(
    f"{base_path}/results"
)

drivers = spark.read.parquet(
    f"{base_path}/drivers"
)

constructors = spark.read.parquet(
    f"{base_path}/constructors"
)

races = spark.read.parquet(
    f"{base_path}/races"
)


drivers_gold = (
    drivers
    .select(
        "driver_id",
        concat_ws(
            " ",
            col("given_name"),
            col("family_name")
        ).alias("driver_name"),
        col("nationality").alias("driver_nationality")
    )
)


constructors_gold = (
    constructors
    .select(
        "constructor_id",
        col("name").alias("constructor_name")
    )
)


races_gold = (
    races
    .select(
        "season",
        "round",
        "circuit_name",
        "country"
    )
)


gold = (
    results

    .join(
        drivers_gold,
        on="driver_id",
        how="left"
    )

    .join(
        constructors_gold,
        on="constructor_id",
        how="left"
    )

    .join(
        races_gold,
        on=["season", "round"],
        how="left"
    )
)


gold = gold.select(
    "season",
    "round",
    "race_name",
    "race_date",
    "circuit_id",
    "circuit_name",
    "country",

    "driver_id",
    "driver_name",
    "driver_nationality",

    "constructor_id",
    "constructor_name",

    "grid",
    "position",
    "position_text",
    "points",
    "laps",
    "status",
    "fastest_lap_rank"
)


print("Schema Gold:")
gold.printSchema()

gold.orderBy(
    "round",
    "position"
).show(
    30,
    truncate=False
)


gold.write.mode("overwrite").parquet(
    output_path
)


print()
print(
    f"Gold Spark construída com sucesso "
    f"para {SEASON}"
)


spark.stop()