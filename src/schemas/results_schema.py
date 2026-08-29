from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType
)


driver_schema = StructType([
    StructField("driverId", StringType(), True),
    StructField("permanentNumber", StringType(), True),
    StructField("code", StringType(), True),
    StructField("givenName", StringType(), True),
    StructField("familyName", StringType(), True),
    StructField("dateOfBirth", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])


constructor_schema = StructType([
    StructField("constructorId", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])


fastest_lap_time_schema = StructType([
    StructField("time", StringType(), True)
])


fastest_lap_schema = StructType([
    StructField("Time", fastest_lap_time_schema, True),
    StructField("lap", StringType(), True),
    StructField("rank", StringType(), True)
])


time_schema = StructType([
    StructField("millis", StringType(), True),
    StructField("time", StringType(), True)
])


result_schema = StructType([
    StructField("Constructor", constructor_schema, True),
    StructField("Driver", driver_schema, True),
    StructField("FastestLap", fastest_lap_schema, True),
    StructField("Time", time_schema, True),
    StructField("grid", StringType(), True),
    StructField("laps", StringType(), True),
    StructField("number", StringType(), True),
    StructField("points", StringType(), True),
    StructField("position", StringType(), True),
    StructField("positionText", StringType(), True),
    StructField("status", StringType(), True)
])


race_result_schema = StructType([
    StructField("circuitId", StringType(), True),
    StructField("date", StringType(), True),
    StructField("raceName", StringType(), True),
    StructField("result", result_schema, True),
    StructField("round", StringType(), True),
    StructField("season", StringType(), True)
])


results_bronze_schema = StructType([
    StructField(
        "results",
        ArrayType(race_result_schema),
        True
    ),
    StructField("season", StringType(), True),
    StructField("total", LongType(), True)
])