from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType
)


driver_schema = StructType([
    StructField("code", StringType(), True),
    StructField("dateOfBirth", StringType(), True),
    StructField("driverId", StringType(), True),
    StructField("familyName", StringType(), True),
    StructField("givenName", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("permanentNumber", StringType(), True),
    StructField("url", StringType(), True)
])


drivers_bronze_schema = StructType([
    StructField(
        "drivers",
        ArrayType(driver_schema),
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