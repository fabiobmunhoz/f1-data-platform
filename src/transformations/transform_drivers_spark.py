import sys
from pathlib import Path

from pyspark.sql.functions import col, to_date

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from schemas.drivers_schema import drivers_bronze_schema
from logger import get_logger
from spark_utils import create_spark_session
from config import (
    get_bronze_path,
    get_silver_path
)


logger = get_logger("transform_drivers")


if len(sys.argv) < 2:
    print("Uso: python transform_drivers_spark.py <season>")
    sys.exit(1)


SEASON = sys.argv[1]

logger.info(
    f"Iniciando transformação Spark de drivers | season={SEASON}"
)


input_path = str(
    get_bronze_path(
        SEASON,
        "drivers"
    )
)

output_path = str(
    get_silver_path(
        SEASON,
        "drivers"
    )
)


spark = create_spark_session(
    "F1DriversTransformation"
)


try:

    df_raw = (
        spark.read
        .schema(drivers_bronze_schema)
        .option("multiLine", "true")
        .json(input_path)
    )

    print("Schema Bronze:")
    df_raw.printSchema()


    drivers = (
        df_raw
        .selectExpr("explode(drivers) as driver")
        .select(
            col("driver.driverId").alias("driver_id"),
            col("driver.permanentNumber").alias("permanent_number"),
            col("driver.code").alias("code"),
            col("driver.givenName").alias("given_name"),
            col("driver.familyName").alias("family_name"),
            col("driver.dateOfBirth").alias("date_of_birth"),
            col("driver.nationality").alias("nationality")
        )
    )


    drivers = drivers.withColumn(
        "date_of_birth",
        to_date(col("date_of_birth"))
    )


    drivers = drivers.dropDuplicates(
        ["driver_id"]
    )


    total_records = drivers.count()

    logger.info(
        f"Transformação concluída | season={SEASON} | records={total_records}"
    )


    drivers.printSchema()

    drivers.show(
        10,
        truncate=False
    )


    logger.info(
        f"Salvando Silver | path={output_path}"
    )


    drivers.write.mode(
        "overwrite"
    ).parquet(
        output_path
    )


    logger.info(
        f"Silver salva com sucesso | season={SEASON}"
    )


except Exception:

    logger.exception(
        f"Falha na transformação de drivers | season={SEASON}"
    )

    raise


finally:

    spark.stop()