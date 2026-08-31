import sys
from pathlib import Path

from pyspark.sql.functions import (
    col,
    explode,
    concat_ws,
    transform
)

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from schemas.driver_standings_schema import (
    driver_standings_bronze_schema
)
from spark_utils import create_spark_session
from config import (
    get_bronze_path,
    get_silver_path
)
from logger import get_logger


logger = get_logger("transform_driver_standings")


if len(sys.argv) < 2:
    print(
        "Uso: python transform_driver_standings_spark.py <season>"
    )
    sys.exit(1)


SEASON = sys.argv[1]


logger.info(
    f"Iniciando transformação Spark de driver standings | "
    f"season={SEASON}"
)


input_path = str(
    get_bronze_path(
        SEASON,
        "driver_standings"
    )
)

output_path = str(
    get_silver_path(
        SEASON,
        "driver_standings"
    )
)


spark = create_spark_session(
    "F1DriverStandingsTransformation"
)


try:

    df_raw = (
        spark.read
        .schema(driver_standings_bronze_schema)
        .option("multiLine", "true")
        .json(input_path)
    )


    # Abre o primeiro array:
    # standings
    standings = (
        df_raw
        .select(
            explode(
                col("standings")
            ).alias("standing")
        )
    )


    # Abre o segundo array:
    # DriverStandings
    driver_standings = (
        standings
        .select(
            col("standing.season")
                .alias("season"),

            col("standing.round")
                .alias("round"),

            explode(
                col("standing.DriverStandings")
            ).alias("driver_standing")
        )
    )


    final_df = (
        driver_standings
        .select(
            col("season")
                .cast("int")
                .alias("season"),

            col("round")
                .cast("int")
                .alias("round"),

            col("driver_standing.position")
                .cast("int")
                .alias("position"),

            col("driver_standing.points")
                .cast("double")
                .alias("points"),

            col("driver_standing.wins")
                .cast("int")
                .alias("wins"),

            col("driver_standing.Driver.driverId")
                .alias("driver_id"),

            concat_ws(
                " ",
                col("driver_standing.Driver.givenName"),
                col("driver_standing.Driver.familyName")
            ).alias("driver_name"),

            col("driver_standing.Driver.code")
                .alias("driver_code"),

            col("driver_standing.Driver.nationality")
                .alias("driver_nationality"),

            transform(
                col("driver_standing.Constructors"),
                lambda x: x["constructorId"]
            ).alias("constructor_ids"),

            transform(
                col("driver_standing.Constructors"),
                lambda x: x["name"]
            ).alias("constructor_names")
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
        "position"
    ).show(
        30,
        truncate=False
    )


    logger.info(
        f"Salvando Silver | path={output_path}"
    )


    final_df.write.mode(
        "overwrite"
    ).parquet(
        output_path
    )


    logger.info(
        f"Silver salva com sucesso | season={SEASON}"
    )


except Exception:

    logger.exception(
        f"Falha na transformação de driver standings | "
        f"season={SEASON}"
    )

    raise


finally:

    spark.stop()