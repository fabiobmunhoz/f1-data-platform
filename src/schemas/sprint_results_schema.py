from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType
)


driver_schema = StructType([
    StructField("driverId", StringType(), True),
    StructField("permanentNumber", StringType(), True),
    StructField("code", StringType(), True),
    StructField("givenName", StringType(), True),
    StructField("familyName", StringType(), True),
    StructField("dateOfBirth", StringType(), True),
    StructField("nationality", StringType(), True),
])


constructor_schema = StructType([
    StructField("constructorId", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
])


fastest_lap_time_schema = StructType([
    StructField("time", StringType(), True)
])


fastest_lap_schema = StructType([
    StructField("rank", StringType(), True),
    StructField("lap", StringType(), True),
    StructField("Time", fastest_lap_time_schema, True),
])


sprint_result_schema = StructType([
    StructField("number", StringType(), True),
    StructField("position", StringType(), True),
    StructField("positionText", StringType(), True),
    StructField("points", StringType(), True),
    StructField("Driver", driver_schema, True),
    StructField("Constructor", constructor_schema, True),
    StructField("grid", StringType(), True),
    StructField("laps", StringType(), True),
    StructField("status", StringType(), True),
    StructField("FastestLap", fastest_lap_schema, True),
])


race_schema = StructType([
    StructField("season", StringType(), True),
    StructField("round", StringType(), True),
    StructField("raceName", StringType(), True),
    StructField("date", StringType(), True),

    StructField(
        "SprintResults",
        ArrayType(sprint_result_schema),
        True
    ),
])


sprint_results_bronze_schema = StructType([
    StructField("season", StringType(), True),
    StructField("total", StringType(), True),

    StructField(
        "races",
        ArrayType(race_schema),
        True
    ),
])