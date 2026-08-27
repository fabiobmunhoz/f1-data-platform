import sys


from pyspark.sql.functions import col
from pathlib import Path

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from spark_utils import create_spark_session

if len(sys.argv) < 2:
    print("Uso: python transform_constructors_spark.py <season>")
    sys.exit(1)

SEASON = sys.argv[1]

input_path = f"data/bronze/season={SEASON}/constructors.json"
output_path = f"data/silver_spark/season={SEASON}/constructors"


spark = create_spark_session(
"F1ConstructorsTransformation"
)


df_raw = (
    spark.read
    .option("multiLine", "true")
    .json(input_path)
)


constructors = (
    df_raw
    .selectExpr("explode(constructors) as constructor")
    .select(
        col("constructor.constructorId").alias("constructor_id"),
        col("constructor.name").alias("name"),
        col("constructor.nationality").alias("nationality")
    )
    .dropDuplicates(["constructor_id"])
)


print("Schema Silver:")
constructors.printSchema()

constructors.show(
    truncate=False
)


constructors.write.mode("overwrite").parquet(
    output_path
)


print()
print(
    f"Transformação Spark de construtores "
    f"concluída para {SEASON}"
)


spark.stop()