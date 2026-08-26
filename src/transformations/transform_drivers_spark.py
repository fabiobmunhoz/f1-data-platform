import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


if len(sys.argv) < 2:
    print("Uso: python transform_drivers_spark.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

input_path = f"data/bronze/season={SEASON}/drivers.json"
output_path = f"data/silver_spark/season={SEASON}/drivers"


spark = (
    SparkSession.builder
    .appName("F1DriversTransformation")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)


df_raw = (
    spark.read
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


drivers.printSchema()

drivers.show(
    10,
    truncate=False
)


Path(
    f"data/silver_spark/season={SEASON}"
).mkdir(
    parents=True,
    exist_ok=True
)


drivers.write.mode("overwrite").parquet(
    output_path
)


print()
print(f"Transformação Spark concluída para {SEASON}")


spark.stop()