from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType
)


location_schema = StructType([
    StructField("lat", StringType(), True),
    StructField("long", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("country", StringType(), True)
])


circuit_schema = StructType([
    StructField("circuitId", StringType(), True),
    StructField("circuitName", StringType(), True),
    StructField("url", StringType(), True),
    StructField("Location", location_schema, True)
])


race_schema = StructType([
    StructField("season", StringType(), True),
    StructField("round", StringType(), True),
    StructField("raceName", StringType(), True),
    StructField("date", StringType(), True),
    StructField("url", StringType(), True),
    StructField("Circuit", circuit_schema, True)
])


races_bronze_schema = StructType([
    StructField(
        "races",
        ArrayType(race_schema),
        True
    ),
    StructField(
        "season",
        StringType(),
        True
    ),
    StructField(
        "total",
        LongType(),
        True
    )
])