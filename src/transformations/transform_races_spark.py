import sys

from pyspark.sql.functions import col, to_date
from pathlib import Path

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from spark_utils import create_spark_session

if len(sys.argv) < 2:
    print("Uso: python transform_races_spark.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

input_path = f"data/bronze/season={SEASON}/races.json"
output_path = f"data/silver_spark/season={SEASON}/races"


spark = create_spark_session(  
    "F1RacesTransformation"
)


df_raw = (
    spark.read
    .option("multiLine", "true")
    .json(input_path)
)


races = (
    df_raw
    .selectExpr("explode(races) as race")
    .select(
        col("race.season").cast("int").alias("season"),
        col("race.round").cast("int").alias("round"),
        col("race.raceName").alias("race_name"),
        to_date(col("race.date")).alias("date"),

        col("race.Circuit.circuitId").alias("circuit_id"),
        col("race.Circuit.circuitName").alias("circuit_name"),

        col("race.Circuit.Location.locality").alias("locality"),
        col("race.Circuit.Location.country").alias("country"),

        col("race.Circuit.Location.lat")
            .cast("double")
            .alias("latitude"),

        col("race.Circuit.Location.long")
            .cast("double")
            .alias("longitude")
    )
    .dropDuplicates(["season", "round"])
)


print("Schema Silver:")
races.printSchema()

races.orderBy("round").show(
    truncate=False
)


races.write.mode("overwrite").parquet(
    output_path
)


print()
print(
    f"Transformação Spark de corridas "
    f"concluída para {SEASON}"
)


spark.stop()