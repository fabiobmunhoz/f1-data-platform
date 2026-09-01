import sys
from pathlib import Path

from pyspark.sql.functions import col, explode, to_date

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from schemas.results_schema import results_bronze_schema
from spark_utils import create_spark_session
from config import (
    get_bronze_path,
    get_silver_path,
    to_spark_path
)
from logger import get_logger


logger = get_logger("transform_results")


if len(sys.argv) < 2:
    print("Uso: python transform_results_spark.py <season>")
    sys.exit(1)


SEASON = sys.argv[1]


logger.info(
    f"Iniciando transformação Spark de resultados | season={SEASON}"
)


input_path = to_spark_path(
    get_bronze_path(
        SEASON,
        "results"
    )
)


output_path = to_spark_path(
    get_silver_path(
        SEASON,
        "results"
    )
)


spark = create_spark_session(
    "F1ResultsTransformation"
)


try:

    df_raw = (
        spark.read
        .schema(results_bronze_schema)
        .option("multiLine", "true")
        .json(input_path)
    )


    race_results = (
        df_raw
        .select(
            explode(
                col("results")
            ).alias("race_result")
        )
    )


    results = (
        race_results
        .select(
            col("race_result.season")
                .cast("int")
                .alias("season"),

            col("race_result.round")
                .cast("int")
                .alias("round"),

            col("race_result.raceName")
                .alias("race_name"),

            to_date(
                col("race_result.date")
            ).alias("race_date"),

            col("race_result.circuitId")
                .alias("circuit_id"),

            col("race_result.result.Driver.driverId")
                .alias("driver_id"),

            col("race_result.result.Constructor.constructorId")
                .alias("constructor_id"),

            col("race_result.result.grid")
                .cast("int")
                .alias("grid"),

            col("race_result.result.position")
                .cast("int")
                .alias("position"),

            col("race_result.result.positionText")
                .alias("position_text"),

            col("race_result.result.points")
                .cast("double")
                .alias("points"),

            col("race_result.result.laps")
                .cast("int")
                .alias("laps"),

            col("race_result.result.status")
                .alias("status"),

            col("race_result.result.FastestLap.rank")
                .cast("int")
                .alias("fastest_lap_rank")
        )
        .dropDuplicates(
            [
                "season",
                "round",
                "driver_id"
            ]
        )
    )


    total_records = results.count()


    logger.info(
        f"Transformação concluída | "
        f"season={SEASON} | "
        f"records={total_records}"
    )


    print("Schema Silver:")
    results.printSchema()


    results.orderBy(
        "round",
        "position"
    ).show(
        30,
        truncate=False
    )


    logger.info(
        f"Salvando Silver | path={output_path}"
    )


    (
        results.write
        .mode("overwrite")
        .parquet(output_path)
    )


    logger.info(
        f"Silver salva com sucesso | season={SEASON}"
    )


except Exception:

    logger.exception(
        f"Falha na transformação de resultados | season={SEASON}"
    )

    raise


finally:

    spark.stop()