"""Spark session and read/write helpers."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import JAR_PATH, JDBC_URL, JDBC_PROPS


def get_spark(app_name):
    """Start Spark with the Postgres jar attached."""
    jar = str(JAR_PATH)
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars", jar)
        .config("spark.driver.extraClassPath", jar)
        .config("spark.driver.memory", "6g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_csv(spark, path, schema):
    """
    Read a CSV file.

    quote and escape handle titles like "American President, The (1995)"
    where a comma sits inside quotes.
    """
    return (
        spark.read
        .option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(str(path))
    )


def add_load_timestamp(df):
    """Add the load_tstmp column required for bronze tables."""
    return df.withColumn("load_tstmp", F.current_timestamp())


def write_to_postgres(df, table, mode):
    # cache() tells Spark: after you compute this once, keep a copy
    # in memory. Without it, .count() below and .write() further down
    df = df.cache()

    row_count = df.count()

    df.coalesce(4).write.jdbc(
        url=JDBC_URL, table=table, mode=mode, properties=JDBC_PROPS
    )

    # release the memory now that both the count and the write are done
    df.unpersist()

    return row_count