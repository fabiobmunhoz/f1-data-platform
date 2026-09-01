import os

from pyspark.sql import SparkSession


def create_spark_session(app_name):

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
    )

    if os.getenv("DATA_STORAGE", "local") == "s3":

        builder = (
            builder
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.1"
            )
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider"
            )
        )

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark