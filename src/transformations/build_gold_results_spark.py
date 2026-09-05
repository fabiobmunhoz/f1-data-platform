import sys
from pathlib import Path

from pyspark.sql.functions import col, concat_ws

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from spark_utils import create_spark_session

from config import (
    get_silver_path,
    get_gold_path,
    to_spark_path
)

from logger import get_logger


logger = get_logger("build_gold_results")


if len(sys.argv) < 2:
    print("Uso: python build_gold_results_spark.py <season>")
    sys.exit(1)


SEASON = sys.argv[1]


logger.info(
    f"Iniciando construção Gold Spark | season={SEASON}"
)


results_path = to_spark_path(
    get_silver_path(
        SEASON,
        "results"
    )
)

drivers_path = to_spark_path(
    get_silver_path(
        SEASON,
        "drivers"
    )
)

constructors_path = to_spark_path(
    get_silver_path(
        SEASON,
        "constructors"
    )
)

races_path = to_spark_path(
    get_silver_path(
        SEASON,
        "races"
    )
)

output_path = to_spark_path(
    get_gold_path(
        SEASON,
        "fact_race_results"
    )
)


spark = create_spark_session(
    "F1GoldResults"
)


try:

    results = spark.read.parquet(
        results_path
    )

    drivers = spark.read.parquet(
        drivers_path
    )

    constructors = spark.read.parquet(
        constructors_path
    )

    races = spark.read.parquet(
        races_path
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

            col("nationality")
                .alias("driver_nationality")
        )
    )


    constructors_gold = (
        constructors
        .select(
            "constructor_id",

            col("name")
                .alias("constructor_name")
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
            on=[
                "season",
                "round"
            ],
            how="left"
        )
    )


    gold = (
        gold
        .select(
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
    )


    total_records = gold.count()


    logger.info(
        f"Gold construída | "
        f"season={SEASON} | "
        f"records={total_records}"
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


    logger.info(
        f"Salvando Gold | path={output_path}"
    )


    (
        gold.write
        .mode("overwrite")
        .parquet(output_path)
    )


    logger.info(
        f"Gold salva com sucesso | season={SEASON}"
    )


except Exception:

    logger.exception(
        f"Falha na construção da Gold | season={SEASON}"
    )

    raise


finally:

    spark.stop()