import sys
from pathlib import Path

from pyspark.sql.functions import col

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from schemas.constructors_schema import constructors_bronze_schema
from spark_utils import create_spark_session
from config import (
    get_bronze_path,
    get_silver_path,
    to_spark_path
)
from logger import get_logger


logger = get_logger("transform_constructors")


if len(sys.argv) < 2:
    print("Uso: python transform_constructors_spark.py <season>")
    sys.exit(1)


SEASON = sys.argv[1]


logger.info(
    f"Iniciando transformação Spark de construtores | season={SEASON}"
)


input_path = to_spark_path(
    get_bronze_path(
        SEASON,
        "constructors"
    )
)


output_path = to_spark_path(
    get_silver_path(
        SEASON,
        "constructors"
    )
)


spark = create_spark_session(
    "F1ConstructorsTransformation"
)


try:

    df_raw = (
        spark.read
        .schema(constructors_bronze_schema)
        .option("multiLine", "true")
        .json(input_path)
    )


    constructors = (
        df_raw
        .selectExpr(
            "explode(constructors) as constructor"
        )
        .select(
            col("constructor.constructorId")
                .alias("constructor_id"),

            col("constructor.name")
                .alias("name"),

            col("constructor.nationality")
                .alias("nationality")
        )
        .dropDuplicates(
            ["constructor_id"]
        )
    )


    total_records = constructors.count()


    logger.info(
        f"Transformação concluída | "
        f"season={SEASON} | "
        f"records={total_records}"
    )


    print("Schema Silver:")

    constructors.printSchema()


    constructors.show(
        truncate=False
    )


    logger.info(
        f"Salvando Silver | path={output_path}"
    )


    (
        constructors.write
        .mode("overwrite")
        .parquet(output_path)
    )


    logger.info(
        f"Silver salva com sucesso | season={SEASON}"
    )


except Exception:

    logger.exception(
        f"Falha na transformação de construtores | season={SEASON}"
    )

    raise


finally:

    spark.stop()