import sys
from pathlib import Path

from pyspark.sql.functions import (
    col,
    explode
)

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from schemas.constructor_standings_schema import (
    constructor_standings_bronze_schema
)
from spark_utils import create_spark_session
from config import (
    get_bronze_path,
    get_silver_path
)
from logger import get_logger


logger = get_logger("transform_constructor_standings")


if len(sys.argv) < 2:
    print(
        "Uso: python transform_constructor_standings_spark.py <season>"
    )
    sys.exit(1)


SEASON = sys.argv[1]


logger.info(
    f"Iniciando transformação Spark de constructor standings | "
    f"season={SEASON}"
)


input_path = str(
    get_bronze_path(
        SEASON,
        "constructor_standings"
    )
)

output_path = str(
    get_silver_path(
        SEASON,
        "constructor_standings"
    )
)


spark = create_spark_session(
    "F1ConstructorStandingsTransformation"
)


try:

    df_raw = (
        spark.read
        .schema(constructor_standings_bronze_schema)
        .option("multiLine", "true")
        .json(input_path)
    )


    standings = (
        df_raw
        .select(
            explode(
                col("standings")
            ).alias("standing")
        )
    )


    constructor_standings = (
        standings
        .select(
            col("standing.season")
                .alias("season"),

            col("standing.round")
                .alias("round"),

            explode(
                col("standing.ConstructorStandings")
            ).alias("constructor_standing")
        )
    )


    final_df = (
        constructor_standings
        .select(
            col("season")
                .cast("int")
                .alias("season"),

            col("round")
                .cast("int")
                .alias("round"),

            col("constructor_standing.position")
                .cast("int")
                .alias("position"),

            col("constructor_standing.points")
                .cast("double")
                .alias("points"),

            col("constructor_standing.wins")
                .cast("int")
                .alias("wins"),

            col("constructor_standing.Constructor.constructorId")
                .alias("constructor_id"),

            col("constructor_standing.Constructor.name")
                .alias("constructor_name"),

            col("constructor_standing.Constructor.nationality")
                .alias("constructor_nationality")
        )
        .dropDuplicates(
            [
                "season",
                "round",
                "constructor_id"
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
        20,
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
        f"Falha na transformação de constructor standings | "
        f"season={SEASON}"
    )

    raise


finally:

    spark.stop()