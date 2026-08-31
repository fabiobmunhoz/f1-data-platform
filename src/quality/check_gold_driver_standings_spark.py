import sys
from pathlib import Path

from pyspark.sql.functions import (
    col,
    count
)

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from spark_utils import create_spark_session
from config import get_gold_path
from logger import get_logger


logger = get_logger("check_gold_driver_standings")


if len(sys.argv) < 2:
    print(
        "Uso: python check_gold_driver_standings_spark.py <season>"
    )
    sys.exit(1)


SEASON = sys.argv[1]


input_path = str(
    get_gold_path(
        SEASON,
        "driver_standings"
    )
)


spark = create_spark_session(
    "F1QualityDriverStandings"
)


try:

    logger.info(
        f"Iniciando quality check de driver standings | "
        f"season={SEASON}"
    )


    df = (
        spark.read
        .parquet(input_path)
    )


    total_records = df.count()


    if total_records == 0:
        raise ValueError(
            "Gold driver_standings está vazia."
        )


    null_driver_id = (
        df
        .filter(
            col("driver_id").isNull()
        )
        .count()
    )


    if null_driver_id > 0:
        raise ValueError(
            f"Encontrados {null_driver_id} registros "
            f"com driver_id nulo."
        )


    duplicate_drivers = (
        df
        .groupBy(
            "season",
            "round",
            "driver_id"
        )
        .agg(
            count("*").alias("total")
        )
        .filter(
            col("total") > 1
        )
        .count()
    )


    if duplicate_drivers > 0:
        raise ValueError(
            f"Encontradas {duplicate_drivers} duplicidades "
            f"por season/round/driver_id."
        )


    invalid_positions = (
        df
        .filter(
            (col("position").isNull()) |
            (col("position") <= 0)
        )
        .count()
    )


    if invalid_positions > 0:
        raise ValueError(
            f"Encontradas {invalid_positions} posições inválidas."
        )


    duplicate_positions = (
        df
        .groupBy(
            "season",
            "round",
            "position"
        )
        .agg(
            count("*").alias("total")
        )
        .filter(
            col("total") > 1
        )
        .count()
    )


    if duplicate_positions > 0:
        raise ValueError(
            f"Encontradas {duplicate_positions} posições duplicadas."
        )


    logger.info(
        f"Quality check concluído com sucesso | "
        f"season={SEASON} | "
        f"records={total_records}"
    )


    print()
    print("=" * 60)
    print("QUALITY CHECK - DRIVER STANDINGS")
    print("=" * 60)
    print(f"Temporada: {SEASON}")
    print(f"Registros: {total_records}")
    print("driver_id nulo: 0")
    print("duplicidade de pilotos: 0")
    print("posições inválidas: 0")
    print("posições duplicadas: 0")
    print("STATUS: OK")
    print("=" * 60)


except Exception:

    logger.exception(
        f"Falha no quality check de driver standings | "
        f"season={SEASON}"
    )

    raise


finally:

    spark.stop()