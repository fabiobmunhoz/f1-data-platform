import sys
from pathlib import Path

from pyspark.sql.functions import (
    col,
    abs
)

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from spark_utils import create_spark_session
from config import get_gold_path
from logger import get_logger


logger = get_logger("check_driver_points_reconciliation")


if len(sys.argv) < 2:
    print(
        "Uso: python check_driver_points_reconciliation_spark.py <season>"
    )
    sys.exit(1)


SEASON = sys.argv[1]


stats_path = str(
    get_gold_path(
        SEASON,
        "driver_season_stats"
    )
)

standings_path = str(
    get_gold_path(
        SEASON,
        "driver_standings"
    )
)


spark = create_spark_session(
    "F1DriverPointsReconciliation"
)


try:

    logger.info(
        f"Iniciando reconciliação de pontos | season={SEASON}"
    )


    stats_df = (
        spark.read
        .parquet(stats_path)
        .select(
            "season",
            "driver_id",
            "driver_name",
            col("total_points").alias("calculated_points")
        )
    )


    standings_df = (
        spark.read
        .parquet(standings_path)
        .select(
            "season",
            "driver_id",
            col("points").alias("official_points")
        )
    )


    reconciliation = (
        stats_df
        .join(
            standings_df,
            on=[
                "season",
                "driver_id"
            ],
            how="full"
        )
        .withColumn(
            "points_difference",
            col("calculated_points")
            - col("official_points")
        )
    )


    print()
    print("Reconciliação de pontos:")

    reconciliation.orderBy(
        col("calculated_points").desc()
    ).show(
        30,
        truncate=False
    )


    missing_records = (
        reconciliation
        .filter(
            col("calculated_points").isNull()
            | col("official_points").isNull()
        )
        .count()
    )


    if missing_records > 0:
        raise ValueError(
            f"Reconciliação falhou: "
            f"{missing_records} pilotos não existem nos dois datasets."
        )


    divergent_points = (
        reconciliation
        .filter(
            abs(
                col("points_difference")
            ) > 0.001
        )
        .count()
    )


    if divergent_points > 0:

        print()
        print("Pilotos com divergência:")

        reconciliation.filter(
            abs(
                col("points_difference")
            ) > 0.001
        ).show(
            truncate=False
        )

        raise ValueError(
            f"Reconciliação falhou: "
            f"{divergent_points} pilotos possuem diferença de pontos."
        )


    total_records = reconciliation.count()


    logger.info(
        f"Reconciliação concluída com sucesso | "
        f"season={SEASON} | "
        f"drivers={total_records}"
    )


    print()
    print("=" * 60)
    print("QUALITY CHECK - DRIVER POINTS RECONCILIATION")
    print("=" * 60)
    print(f"Temporada: {SEASON}")
    print(f"Pilotos comparados: {total_records}")
    print("Registros ausentes: 0")
    print("Divergências de pontos: 0")
    print("STATUS: OK")
    print("=" * 60)


except Exception:

    logger.exception(
        f"Falha na reconciliação de pontos | season={SEASON}"
    )

    raise


finally:

    spark.stop()