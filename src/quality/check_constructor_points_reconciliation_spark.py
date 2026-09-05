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
from config import (
    get_gold_path,
    to_spark_path
)
from logger import get_logger


logger = get_logger(
    "check_constructor_points_reconciliation"
)


if len(sys.argv) < 2:
    print(
        "Uso: python "
        "check_constructor_points_reconciliation_spark.py <season>"
    )
    sys.exit(1)


SEASON = sys.argv[1]


stats_path = to_spark_path(
    get_gold_path(
        SEASON,
        "constructor_season_stats"
    )
)

standings_path = to_spark_path(
    get_gold_path(
        SEASON,
        "constructor_standings"
    )
)


spark = create_spark_session(
    "F1ConstructorPointsReconciliation"
)


try:

    logger.info(
        f"Iniciando reconciliação de pontos dos construtores | "
        f"season={SEASON}"
    )


    stats_df = (
        spark.read
        .parquet(stats_path)
        .select(
            "season",
            "constructor_id",
            "constructor_name",
            col("total_points")
                .alias("calculated_points")
        )
    )


    standings_df = (
        spark.read
        .parquet(standings_path)
        .select(
            "season",
            "constructor_id",
            col("points")
                .alias("official_points")
        )
    )


    reconciliation = (
        stats_df
        .join(
            standings_df,
            on=[
                "season",
                "constructor_id"
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
    print("Reconciliação de pontos dos construtores:")


    reconciliation.orderBy(
        col("calculated_points").desc()
    ).show(
        20,
        truncate=False
    )


    # ========================================================
    # VERIFICA SE EXISTE CONSTRUTOR AUSENTE
    # ========================================================

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
            f"{missing_records} construtores não existem "
            f"nos dois datasets."
        )


    # ========================================================
    # VERIFICA DIVERGÊNCIA DE PONTOS
    # ========================================================

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
        print("Construtores com divergência:")


        reconciliation.filter(
            abs(
                col("points_difference")
            ) > 0.001
        ).show(
            truncate=False
        )


        raise ValueError(
            f"Reconciliação falhou: "
            f"{divergent_points} construtores possuem "
            f"diferença de pontos."
        )


    total_records = reconciliation.count()


    logger.info(
        f"Reconciliação concluída com sucesso | "
        f"season={SEASON} | "
        f"constructors={total_records}"
    )


    print()
    print("=" * 60)
    print("QUALITY CHECK - CONSTRUCTOR POINTS RECONCILIATION")
    print("=" * 60)
    print(f"Temporada: {SEASON}")
    print(f"Construtores comparados: {total_records}")
    print("Registros ausentes: 0")
    print("Divergências de pontos: 0")
    print("STATUS: OK")
    print("=" * 60)


except Exception:

    logger.exception(
        f"Falha na reconciliação de pontos dos construtores | "
        f"season={SEASON}"
    )

    raise


finally:

    spark.stop()