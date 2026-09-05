import sys
from pathlib import Path

from pyspark.sql.functions import col, count

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from spark_utils import create_spark_session
from config import (
    get_gold_path,
    to_spark_path
)
from logger import get_logger


logger = get_logger("check_gold_constructor_standings")


if len(sys.argv) < 2:
    print(
        "Uso: python check_gold_constructor_standings_spark.py <season>"
    )
    sys.exit(1)


SEASON = sys.argv[1]


input_path = to_spark_path(
    get_gold_path(
        SEASON,
        "constructor_standings"
    )
)


spark = create_spark_session(
    "F1QualityConstructorStandings"
)


try:

    logger.info(
        f"Iniciando quality check de constructor standings | "
        f"season={SEASON}"
    )


    df = (
        spark.read
        .parquet(input_path)
    )


    total_records = df.count()


    if total_records == 0:
        raise ValueError(
            "Gold constructor_standings está vazia."
        )


    null_constructor_id = (
        df
        .filter(
            col("constructor_id").isNull()
        )
        .count()
    )


    if null_constructor_id > 0:
        raise ValueError(
            f"Encontrados {null_constructor_id} registros "
            f"com constructor_id nulo."
        )


    duplicate_constructors = (
        df
        .groupBy(
            "season",
            "round",
            "constructor_id"
        )
        .agg(
            count("*").alias("total")
        )
        .filter(
            col("total") > 1
        )
        .count()
    )


    if duplicate_constructors > 0:
        raise ValueError(
            f"Encontradas {duplicate_constructors} duplicidades "
            f"por season/round/constructor_id."
        )


    invalid_positions = (
        df
        .filter(
            col("position").isNull()
            | (col("position") <= 0)
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
    print("QUALITY CHECK - CONSTRUCTOR STANDINGS")
    print("=" * 60)
    print(f"Temporada: {SEASON}")
    print(f"Registros: {total_records}")
    print("constructor_id nulo: 0")
    print("duplicidade de construtores: 0")
    print("posições inválidas: 0")
    print("posições duplicadas: 0")
    print("STATUS: OK")
    print("=" * 60)


except Exception:

    logger.exception(
        f"Falha no quality check de constructor standings | "
        f"season={SEASON}"
    )

    raise


finally:

    spark.stop()