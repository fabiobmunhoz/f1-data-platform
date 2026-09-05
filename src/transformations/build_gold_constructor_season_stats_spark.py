import sys
from pathlib import Path

from pyspark.sql.functions import (
    col,
    countDistinct,
    sum,
    avg,
    min,
    when
)

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from spark_utils import create_spark_session
from config import (
    get_gold_path,
    get_silver_path,
    to_spark_path
)
from logger import get_logger


logger = get_logger("build_gold_constructor_season_stats")


if len(sys.argv) < 2:
    print(
        "Uso: python build_gold_constructor_season_stats_spark.py <season>"
    )
    sys.exit(1)


SEASON = sys.argv[1]


logger.info(
    f"Iniciando Gold constructor season stats | season={SEASON}"
)


race_input_path = to_spark_path(
    get_gold_path(
        SEASON,
        "fact_race_results"
    )
)

sprint_input_path = to_spark_path(
    get_silver_path(
        SEASON,
        "sprint_results"
    )
)

output_path = to_spark_path(
    get_gold_path(
        SEASON,
        "constructor_season_stats"
    )
)


spark = create_spark_session(
    "F1GoldConstructorSeasonStats"
)


try:

    race_df = (
        spark.read
        .parquet(race_input_path)
    )


    sprint_df = (
        spark.read
        .parquet(sprint_input_path)
    )


    # ========================================================
    # MÉTRICAS DE CORRIDAS
    # ========================================================

    race_stats = (
        race_df
        .withColumn(
            "is_win",
            when(
                col("position") == 1,
                1
            ).otherwise(0)
        )
        .withColumn(
            "is_podium",
            when(
                col("position").between(1, 3),
                1
            ).otherwise(0)
        )
        .groupBy(
            "season",
            "constructor_id",
            "constructor_name"
        )
        .agg(
            countDistinct(
                "round"
            ).alias("races"),

            sum(
                "is_win"
            ).alias("race_wins"),

            sum(
                "is_podium"
            ).alias("podiums"),

            sum(
                "points"
            ).alias("race_points"),

            avg(
                "position"
            ).alias("avg_finish_position"),

            min(
                "position"
            ).alias("best_finish")
        )
    )


    # ========================================================
    # MÉTRICAS DE SPRINT
    # ========================================================

    sprint_stats = (
        sprint_df
        .groupBy(
            "season",
            "constructor_id"
        )
        .agg(
            countDistinct(
                "round"
            ).alias("sprints"),

            sum(
                when(
                    col("position") == 1,
                    1
                ).otherwise(0)
            ).alias("sprint_wins"),

            sum(
                "points"
            ).alias("sprint_points")
        )
    )


    # ========================================================
    # CONSOLIDAÇÃO DA TEMPORADA
    # ========================================================

    constructor_stats = (
        race_stats
        .join(
            sprint_stats,
            on=[
                "season",
                "constructor_id"
            ],
            how="left"
        )
        .fillna(
            {
                "sprints": 0,
                "sprint_wins": 0,
                "sprint_points": 0.0
            }
        )
        .withColumn(
            "total_points",
            col("race_points")
            + col("sprint_points")
        )
        .orderBy(
            col("total_points").desc()
        )
    )


    total_records = constructor_stats.count()


    logger.info(
        f"Gold constructor season stats construída | "
        f"season={SEASON} | "
        f"records={total_records}"
    )


    print("Schema Gold:")
    constructor_stats.printSchema()


    constructor_stats.show(
        20,
        truncate=False
    )


    # ========================================================
    # SALVAMENTO
    # ========================================================

    logger.info(
        f"Salvando Gold | path={output_path}"
    )


    (
        constructor_stats.write
        .mode("overwrite")
        .parquet(output_path)
    )


    logger.info(
        f"Gold salva com sucesso | season={SEASON}"
    )


except Exception:

    logger.exception(
        f"Falha na Gold constructor season stats | "
        f"season={SEASON}"
    )

    raise


finally:

    spark.stop()