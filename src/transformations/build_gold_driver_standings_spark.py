import sys
from pathlib import Path

from pyspark.sql.functions import (
    col,
    concat_ws
)

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from spark_utils import create_spark_session
from config import (
    get_silver_path,
    get_gold_path
)
from logger import get_logger


logger = get_logger("build_gold_driver_standings")


if len(sys.argv) < 2:
    print(
        "Uso: python build_gold_driver_standings_spark.py <season>"
    )
    sys.exit(1)


SEASON = sys.argv[1]


logger.info(
    f"Iniciando Gold de driver standings | season={SEASON}"
)


input_path = str(
    get_silver_path(
        SEASON,
        "driver_standings"
    )
)

output_path = str(
    get_gold_path(
        SEASON,
        "driver_standings"
    )
)


spark = create_spark_session(
    "F1GoldDriverStandings"
)


try:

    silver_df = (
        spark.read
        .parquet(input_path)
    )


    gold_df = (
        silver_df
        .select(
            col("season"),

            col("round"),

            col("position"),

            col("driver_id"),

            col("driver_name"),

            col("driver_code"),

            col("driver_nationality"),

            concat_ws(
                ", ",
                col("constructor_names")
            ).alias("constructor_names"),

            col("points"),

            col("wins")
        )
        .dropDuplicates(
            [
                "season",
                "round",
                "driver_id"
            ]
        )
    )


    total_records = gold_df.count()


    logger.info(
        f"Gold construída | "
        f"season={SEASON} | "
        f"records={total_records}"
    )


    print("Schema Gold:")

    gold_df.printSchema()


    gold_df.orderBy(
        "position"
    ).show(
        30,
        truncate=False
    )


    logger.info(
        f"Salvando Gold | path={output_path}"
    )


    gold_df.write.mode(
        "overwrite"
    ).parquet(
        output_path
    )


    logger.info(
        f"Gold salva com sucesso | season={SEASON}"
    )


except Exception:

    logger.exception(
        f"Falha na construção da Gold de driver standings | "
        f"season={SEASON}"
    )

    raise


finally:

    spark.stop()