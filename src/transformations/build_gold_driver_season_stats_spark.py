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
    get_silver_path
)
from logger import get_logger


logger = get_logger("build_gold_driver_season_stats")


if len(sys.argv) < 2:
    print(
        "Uso: python build_gold_driver_season_stats_spark.py <season>"
    )
    sys.exit(1)


SEASON = sys.argv[1]


logger.info(
    f"Iniciando Gold driver season stats | season={SEASON}"
)


# ============================================================
# PATHS
# ============================================================

input_path = str(
    get_gold_path(
        SEASON,
        "fact_race_results"
    )
)

sprint_input_path = str(
    get_silver_path(
        SEASON,
        "sprint_results"
    )
)

output_path = str(
    get_gold_path(
        SEASON,
        "driver_season_stats"
    )
)


spark = create_spark_session(
    "F1GoldDriverSeasonStats"
)


try:

    # ========================================================
    # LEITURA DA FACT DE RESULTADOS
    # ========================================================

    df = (
        spark.read
        .parquet(input_path)
    )
    
    sprint_df = (
        spark.read
        .parquet(sprint_input_path)
    )


    sprint_stats = (
        sprint_df
        .groupBy(
            "season",
            "driver_id"
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
    # CRIAÇÃO DE MÉTRICAS POR CORRIDA
    # ========================================================

    enriched_df = (
        df
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
        .withColumn(
            "is_dnf",
            when(
                col("status") == "Retired",
                1
            ).otherwise(0)
        )
        .withColumn(
            "is_dns",
            when(
                col("status") == "Did not start",
                1
            ).otherwise(0)
        )
        .withColumn(
            "positions_gained",
            when(
                (col("grid") > 0)
                & col("position").isNotNull(),
                col("grid") - col("position")
            )
        )
    )


    # ========================================================
    # AGREGAÇÃO POR PILOTO / TEMPORADA
    # ========================================================

    driver_stats = (
        enriched_df
        .groupBy(
            "season",
            "driver_id",
            "driver_name",
            "driver_nationality"
        )
        .agg(

            countDistinct(
                "round"
            ).alias("races"),

            sum(
                "is_win"
            ).alias("wins"),

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
            ).alias("best_finish"),

            sum(
                "is_dnf"
            ).alias("dnfs"),

            sum(
                "is_dns"
            ).alias("dns"),
            
            avg(
                when(
                    col("grid") > 0,
                    col("grid")
                )
            ).alias("avg_grid_position"),

            sum(
                "positions_gained"
            ).alias("positions_gained")
        )
    )


    # ========================================================
    # ORDENAÇÃO
    # ========================================================

    

    driver_stats = (
        driver_stats
        .join(
            sprint_stats,
            on=[
                "season",
                "driver_id"
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
    )

    driver_stats = (
        driver_stats
            .orderBy(
            col("total_points").desc()
            )
    )

    total_records = driver_stats.count()


    logger.info(
        f"Gold driver season stats construída | "
        f"season={SEASON} | "
        f"records={total_records}"
    )


    print("Schema Gold:")
    driver_stats.printSchema()


    driver_stats.show(
        30,
        truncate=False
    )


    # ========================================================
    # SALVAMENTO
    # ========================================================

    logger.info(
        f"Salvando Gold | path={output_path}"
    )


    driver_stats.write.mode(
        "overwrite"
    ).parquet(
        output_path
    )


    logger.info(
        f"Gold salva com sucesso | season={SEASON}"
    )


except Exception:

    logger.exception(
        f"Falha na Gold driver season stats | "
        f"season={SEASON}"
    )

    raise


finally:

    spark.stop()