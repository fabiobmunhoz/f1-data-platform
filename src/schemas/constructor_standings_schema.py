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


constructor_standing_schema = StructType([
    StructField("position", StringType(), True),
    StructField("positionText", StringType(), True),
    StructField("points", StringType(), True),
    StructField("wins", StringType(), True),

    StructField(
        "Constructor",
        constructor_schema,
        True
    ),
])


standings_list_schema = StructType([
    StructField("season", StringType(), True),
    StructField("round", StringType(), True),

    StructField(
        "ConstructorStandings",
        ArrayType(constructor_standing_schema),
        True
    ),
])


constructor_standings_bronze_schema = StructType([
    StructField("season", StringType(), True),
    StructField("total", StringType(), True),

    StructField(
        "standings",
        ArrayType(standings_list_schema),
        True
    ),
])