import sys
from pathlib import Path

from pyspark.sql.functions import (
    col,
    explode,
    to_date
)

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from schemas.sprint_results_schema import (
    sprint_results_bronze_schema
)
from spark_utils import create_spark_session
from config import (
    get_bronze_path,
    get_silver_path,
    to_spark_path
)
from logger import get_logger


logger = get_logger("transform_sprint_results")


if len(sys.argv) < 2:
    print(
        "Uso: python transform_sprint_results_spark.py <season>"
    )
    sys.exit(1)


SEASON = sys.argv[1]


input_path = to_spark_path(
    get_bronze_path(
        SEASON,
        "sprint_results"
    )
)


output_path = to_spark_path(
    get_silver_path(
        SEASON,
        "sprint_results"
    )
)


spark = create_spark_session(
    "F1SprintResultsTransformation"
)


try:

    logger.info(
        f"Iniciando transformação Spark de Sprint Results | "
        f"season={SEASON}"
    )


    df_raw = (
        spark.read
        .schema(sprint_results_bronze_schema)
        .option("multiLine", "true")
        .json(input_path)
    )


    races = (
        df_raw
        .select(
            explode(
                col("races")
            ).alias("race")
        )
    )


    sprint_results = (
        races
        .select(
            col("race.season")
                .alias("season"),

            col("race.round")
                .alias("round"),

            col("race.raceName")
                .alias("race_name"),

            col("race.date")
                .alias("race_date"),

            explode(
                col("race.SprintResults")
            ).alias("sprint_result")
        )
    )


    final_df = (
        sprint_results
        .select(
            col("season")
                .cast("int")
                .alias("season"),

            col("round")
                .cast("int")
                .alias("round"),

            col("race_name"),

            to_date(
                col("race_date")
            ).alias("race_date"),

            col("sprint_result.Driver.driverId")
                .alias("driver_id"),

            col("sprint_result.Constructor.constructorId")
                .alias("constructor_id"),

            col("sprint_result.grid")
                .cast("int")
                .alias("grid"),

            col("sprint_result.position")
                .cast("int")
                .alias("position"),

            col("sprint_result.positionText")
                .alias("position_text"),

            col("sprint_result.points")
                .cast("double")
                .alias("points"),

            col("sprint_result.laps")
                .cast("int")
                .alias("laps"),

            col("sprint_result.status")
                .alias("status"),

            col("sprint_result.FastestLap.rank")
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


    total_records = final_df.count()


    logger.info(
        f"Transformação concluída | "
        f"season={SEASON} | "
        f"records={total_records}"
    )


    print("Schema Silver:")
    final_df.printSchema()


    final_df.orderBy(
        "round",
        "position"
    ).show(
        50,
        truncate=False
    )


    logger.info(
        f"Salvando Silver | path={output_path}"
    )


    (
        final_df.write
        .mode("overwrite")
        .parquet(output_path)
    )


    logger.info(
        f"Silver salva com sucesso | season={SEASON}"
    )


except Exception:

    logger.exception(
        f"Falha na transformação de Sprint Results | "
        f"season={SEASON}"
    )

    raise


finally:

    spark.stop()