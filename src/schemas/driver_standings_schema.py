from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType
)


constructor_schema = StructType([
    StructField("constructorId", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
])


driver_schema = StructType([
    StructField("driverId", StringType(), True),
    StructField("permanentNumber", StringType(), True),
    StructField("code", StringType(), True),
    StructField("givenName", StringType(), True),
    StructField("familyName", StringType(), True),
    StructField("dateOfBirth", StringType(), True),
    StructField("nationality", StringType(), True),
])


driver_standing_schema = StructType([
    StructField("position", StringType(), True),
    StructField("positionText", StringType(), True),
    StructField("points", StringType(), True),
    StructField("wins", StringType(), True),

    StructField(
        "Driver",
        driver_schema,
        True
    ),

    StructField(
        "Constructors",
        ArrayType(constructor_schema),
        True
    ),
])


standings_list_schema = StructType([
    StructField("season", StringType(), True),
    StructField("round", StringType(), True),

    StructField(
        "DriverStandings",
        ArrayType(driver_standing_schema),
        True
    ),
])


driver_standings_bronze_schema = StructType([
    StructField("season", StringType(), True),
    StructField("total", StringType(), True),

    StructField(
        "standings",
        ArrayType(standings_list_schema),
        True
    ),
])