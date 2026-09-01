import sys
from pathlib import Path

from pyspark.sql.functions import col, to_date

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from schemas.races_schema import races_bronze_schema
from spark_utils import create_spark_session
from config import (
    get_bronze_path,
    get_silver_path,
    to_spark_path
)
from logger import get_logger


logger = get_logger("transform_races")


if len(sys.argv) < 2:
    print("Uso: python transform_races_spark.py <season>")
    sys.exit(1)


SEASON = sys.argv[1]


logger.info(
    f"Iniciando transformação Spark de corridas | season={SEASON}"
)


input_path = to_spark_path(
    get_bronze_path(
        SEASON,
        "races"
    )
)


output_path = to_spark_path(
    get_silver_path(
        SEASON,
        "races"
    )
)


spark = create_spark_session(
    "F1RacesTransformation"
)


try:

    df_raw = (
        spark.read
        .schema(races_bronze_schema)
        .option("multiLine", "true")
        .json(input_path)
    )


    races = (
        df_raw
        .selectExpr(
            "explode(races) as race"
        )
        .select(
            col("race.season")
                .cast("int")
                .alias("season"),

            col("race.round")
                .cast("int")
                .alias("round"),

            col("race.raceName")
                .alias("race_name"),

            to_date(
                col("race.date")
            ).alias("date"),

            col("race.Circuit.circuitId")
                .alias("circuit_id"),

            col("race.Circuit.circuitName")
                .alias("circuit_name"),

            col("race.Circuit.Location.locality")
                .alias("locality"),

            col("race.Circuit.Location.country")
                .alias("country"),

            col("race.Circuit.Location.lat")
                .cast("double")
                .alias("latitude"),

            col("race.Circuit.Location.long")
                .cast("double")
                .alias("longitude")
        )
        .dropDuplicates(
            [
                "season",
                "round"
            ]
        )
    )


    total_records = races.count()


    logger.info(
        f"Transformação concluída | "
        f"season={SEASON} | "
        f"records={total_records}"
    )


    print("Schema Silver:")
    races.printSchema()


    races.orderBy(
        "round"
    ).show(
        truncate=False
    )


    logger.info(
        f"Salvando Silver | path={output_path}"
    )


    (
        races.write
        .mode("overwrite")
        .parquet(output_path)
    )


    logger.info(
        f"Silver salva com sucesso | season={SEASON}"
    )


except Exception:

    logger.exception(
        f"Falha na transformação de corridas | season={SEASON}"
    )

    raise


finally:

    spark.stop()