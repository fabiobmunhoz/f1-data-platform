import sys
from pathlib import Path

from pyspark.sql.functions import col

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from config import get_gold_path
from logger import get_logger
from spark_utils import create_spark_session


logger = get_logger("check_gold_results")


if len(sys.argv) < 2:
    print("Uso: python check_gold_results_spark.py <season>")
    sys.exit(1)


SEASON = sys.argv[1]


logger.info(
    f"Iniciando validação Gold Spark | season={SEASON}"
)


input_path = str(
    get_gold_path(
        SEASON,
        "fact_race_results"
    )
)


spark = create_spark_session(
    "F1GoldQualityCheck"
)


try:

    df = spark.read.parquet(
        input_path
    )


    total_records = df.count()

    logger.info(
        f"Gold carregada | season={SEASON} | records={total_records}"
    )


    if total_records == 0:
        raise ValueError(
            "Falha Gold Spark: tabela vazia."
        )


    duplicate_count = (
        df
        .groupBy(
            "season",
            "round",
            "driver_id"
        )
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )


    logger.info(
        f"Duplicados season/round/driver_id: {duplicate_count}"
    )


    if duplicate_count > 0:
        raise ValueError(
            "Falha Gold Spark: existem registros duplicados."
        )


    critical_columns = [
        "season",
        "round",
        "driver_id",
        "driver_name",
        "constructor_id",
        "constructor_name",
        "race_name"
    ]


    for column in critical_columns:

        null_count = (
            df
            .filter(
                col(column).isNull()
            )
            .count()
        )


        logger.info(
            f"Nulos em {column}: {null_count}"
        )


        if null_count > 0:
            raise ValueError(
                f"Falha Gold Spark: {column} possui valores nulos."
            )


    logger.info(
        f"Gold Spark validada com sucesso | season={SEASON}"
    )


except Exception:

    logger.exception(
        f"Falha na validação Gold Spark | season={SEASON}"
    )

    raise


finally:

    spark.stop()