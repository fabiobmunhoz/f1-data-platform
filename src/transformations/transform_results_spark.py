import sys


from pyspark.sql.functions import col, explode, to_date

from pathlib import Path

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from spark_utils import create_spark_session

if len(sys.argv) < 2:
    print("Uso: python transform_results_spark.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

input_path = f"data/bronze/season={SEASON}/results.json"
output_path = f"data/silver_spark/season={SEASON}/results"


spark = create_spark_session(
    "F1ResultsTransformation"  
)



df_raw = (
    spark.read
    .option("multiLine", "true")
    .json(input_path)
)


races = (
    df_raw
    .select(explode(col("results")).alias("race_result"))
)


results = (
    races
    .select(
        col("race_result.season")
            .cast("int")
            .alias("season"),

        col("race_result.round")
            .cast("int")
            .alias("round"),

        col("race_result.raceName")
            .alias("race_name"),

        to_date(
            col("race_result.date")
        ).alias("race_date"),

        col("race_result.circuitId")
            .alias("circuit_id"),

        col("race_result.result.Driver.driverId")
            .alias("driver_id"),

        col("race_result.result.Constructor.constructorId")
            .alias("constructor_id"),

        col("race_result.result.grid")
            .cast("int")
            .alias("grid"),

        col("race_result.result.position")
            .cast("int")
            .alias("position"),

        col("race_result.result.positionText")
            .alias("position_text"),

        col("race_result.result.points")
            .cast("double")
            .alias("points"),

        col("race_result.result.laps")
            .cast("int")
            .alias("laps"),

        col("race_result.result.status")
            .alias("status"),

        col("race_result.result.FastestLap.rank")
            .cast("int")
            .alias("fastest_lap_rank")
    )
    .dropDuplicates(
        [
            "season",
            "round",
            "driver_id"
        ]
    )
)


print("Schema Silver:")
results.printSchema()

results.orderBy(
    "round",
    "position"
).show(
    30,
    truncate=False
)


results.write.mode("overwrite").parquet(
    output_path
)


print()
print(
    f"Transformação Spark de resultados "
    f"concluída para {SEASON}"
)


spark.stop()