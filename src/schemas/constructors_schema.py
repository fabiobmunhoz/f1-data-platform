from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType
)


constructor_schema = StructType([
    StructField("constructorId", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])


constructors_bronze_schema = StructType([
    StructField(
        "constructors",
        ArrayType(constructor_schema),
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