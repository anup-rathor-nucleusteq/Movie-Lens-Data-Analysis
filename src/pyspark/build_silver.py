"""
Build the four base silver tables from bronze.
I have Given 4 GB to Spark driver memory.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))

import quality as Q
import transforms as T
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType, TimestampType,
)
from config import JDBC_URL, JDBC_PROPS
from db import count_rows, execute
from spark_utils import get_spark, write_to_postgres


# MovieLens has userId 1 to 200948. Used to split the big reads.
USER_ID_MIN = 1
USER_ID_MAX = 200948


def read_small(spark, table):
    """Read a small table in one connection. Fine for links and movies."""
    return spark.read.jdbc(
        url=JDBC_URL,
        table=f"bronze.{table}",
        properties=JDBC_PROPS,
    )


def read_large(spark, table, num_partitions=8):
    """
    Read a large table using 8 parallel connections.
    """
    return spark.read.jdbc(
        url=JDBC_URL,
        table=f"bronze.{table}",
        column="userId",
        lowerBound=USER_ID_MIN,
        upperBound=USER_ID_MAX,
        numPartitions=num_partitions,
        properties=JDBC_PROPS,
    )


# Explicit schemas for the silver output tables.
LINKS_SCHEMA = StructType([
    StructField("MovieId", IntegerType(), True),
    StructField("ImdbId", StringType(), True),
    StructField("TmdbId", StringType(), True),
    StructField("CreateDtTm", TimestampType(), True),
    StructField("UpdateDtTm", TimestampType(), True),
])

MOVIES_SCHEMA = StructType([
    StructField("MovieId", IntegerType(), True),
    StructField("Title", StringType(), True),
    StructField("CleanTitle", StringType(), True),
    StructField("ReleaseYear", IntegerType(), True),
    StructField("Genres", StringType(), True),
    StructField("CreateDtTm", TimestampType(), True),
    StructField("UpdateDtTm", TimestampType(), True),
])

RATINGS_SCHEMA = StructType([
    StructField("UserId", IntegerType(), True),
    StructField("MovieId", IntegerType(), True),
    StructField("Rating", DoubleType(), True),
    StructField("RatingTstmp", TimestampType(), True),
    StructField("CreateDtTm", TimestampType(), True),
    StructField("UpdateDtTm", TimestampType(), True),
])

TAGS_SCHEMA = StructType([
    StructField("UserId", IntegerType(), True),
    StructField("MovieId", IntegerType(), True),
    StructField("TagText", StringType(), True),
    StructField("TagTstmp", TimestampType(), True),
    StructField("CreateDtTm", TimestampType(), True),
    StructField("UpdateDtTm", TimestampType(), True),
])


def apply_schema(df, schema):
    """Cast a Silver DataFrame to its explicit StructType schema."""
    return df.select(
        *[
            F.col(field.name).cast(field.dataType).alias(field.name)
            for field in schema.fields
        ]
    )

def upsert_via_staging(spark, df, table_name, join_keys):
    """
    Upserts data into Postgres using a Staging Table pattern.
    This safely bypasses the 'cannot drop table because views depend on it' error.
    """
    staging_table = f"{table_name}_staging"
    
    # Write incoming data to a temporary staging table
    write_to_postgres(df, staging_table, mode="overwrite")
    
    # Ensure target table exists (creates it without data if missing)
    empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), df.schema)
    write_to_postgres(empty_df, table_name, mode="append")
    
    # Delete existing records that match the new keys
    condition = " AND ".join([f'target."{k}" = staging."{k}"' for k in join_keys])
    delete_sql = f"""
        DELETE FROM {table_name} target
        USING {staging_table} staging
        WHERE {condition};
    """
    execute(delete_sql)
    
    # Insert the new records
    cols = ", ".join([f'"{field.name}"' for field in df.schema.fields])
    insert_sql = f"""
        INSERT INTO {table_name} ({cols})
        SELECT {cols} FROM {staging_table};
    """
    execute(insert_sql)
    
    # Clean up
    execute(f"DROP TABLE {staging_table};")


# silver.links

def build_links(spark):
    print("\n--- silver.links ---")

    df = read_small(spark, "links")

    df = T.rename_columns(df, {
        "movieId": "MovieId",
        "imdbId": "ImdbId",
        "tmdbId": "TmdbId",
    })

    bad_condition = Q.check_nulls(df, ["MovieId"])
    good, bad = Q.split_good_and_bad(df, bad_condition, "NULL_MOVIE_ID")

    good = T.add_audit_columns(good)
    good = T.select_columns(good, [
        "MovieId", "ImdbId", "TmdbId", "CreateDtTm", "UpdateDtTm",
    ])

    good = apply_schema(good, LINKS_SCHEMA)
    
    # UPSERT instead of OVERWRITE
    upsert_via_staging(spark, good, "silver.links", ["MovieId"])
    Q.write_quarantine(bad, "links", mode="append")


# silver.movies

def build_movies(spark):
    print("\n--- silver.movies ---")

    df = read_small(spark, "movies")

    df = T.extract_release_year(df, "title")
    df = T.clean_title(df, "title")

    df = T.rename_columns(df, {
        "movieId": "MovieId",
        "title": "Title",
        "genres": "Genres",
    })

    bad_condition = Q.check_nulls(df, ["MovieId", "Title"])
    good, bad = Q.split_good_and_bad(df, bad_condition, "NULL_KEY")

    good = T.add_audit_columns(good)
    good = T.select_columns(good, [
        "MovieId", "Title", "CleanTitle", "ReleaseYear", "Genres",
        "CreateDtTm", "UpdateDtTm",
    ])

    good = apply_schema(good, MOVIES_SCHEMA)
    
    # UPSERT instead of OVERWRITE
    upsert_via_staging(spark, good, "silver.movies", ["MovieId"])
    Q.write_quarantine(bad, "movies", mode="append")


def read_user_range(spark, table, user_from, user_to):
    query = (
        f'(SELECT * FROM bronze.{table} '
        f'WHERE "userId" >= {user_from} AND "userId" < {user_to}) AS chunk'
    )
    return spark.read.jdbc(url=JDBC_URL, table=query, properties=JDBC_PROPS)

def read_silver_user_range(spark, table, user_from, user_to):
    query = (
        f'(SELECT * FROM silver.{table} '
        f'WHERE "UserId" >= {user_from} AND "UserId" < {user_to}) AS chunk'
    )
    return spark.read.jdbc(url=JDBC_URL, table=query, properties=JDBC_PROPS)


def build_ratings(spark, batch_size=20000):
    print("\n--- silver.ratings ---")

    batch_num = 0
    user_from = USER_ID_MIN

    while user_from <= USER_ID_MAX:
        user_to = user_from + batch_size
        batch_num += 1

        df = read_user_range(spark, "ratings", user_from, user_to)

        df = T.rename_columns(df, {
            "userId": "UserId",
            "movieId": "MovieId",
            "rating": "Rating",
        })

        bad_condition = (
            Q.check_nulls(df, ["UserId", "MovieId", "Rating"])
            | Q.check_rating_range("Rating")
            | Q.check_epoch_range("timestamp")
        )
        good, bad = Q.split_good_and_bad(df, bad_condition, "FAILED_VALIDATION")

        good = T.epoch_to_timestamp(good, "timestamp", "RatingTstmp")
        good = T.dedupe_keep_latest(good, ["UserId", "MovieId"], "timestamp")

        good = T.add_audit_columns(good)
        good = T.select_columns(good, [
            "UserId", "MovieId", "Rating", "RatingTstmp",
            "CreateDtTm", "UpdateDtTm",
        ])

        good = apply_schema(good, RATINGS_SCHEMA)
        
        # UPSERT instead of OVERWRITE
        upsert_via_staging(spark, good, "silver.ratings", ["UserId", "MovieId"])
        Q.write_quarantine(bad, "ratings", mode="append")

        print(f"  batch {batch_num}: users {user_from:,} to {user_to:,}")
        user_from = user_to

    print(f"  done, {batch_num} batches")


def build_tags(spark, batch_size=50000):
    print("\n--- silver.tags ---")

    batch_num = 0
    user_from = USER_ID_MIN

    while user_from <= USER_ID_MAX:
        user_to = user_from + batch_size
        batch_num += 1

        df = read_user_range(spark, "tags", user_from, user_to)

        df = T.rename_columns(df, {
            "userId": "UserId",
            "movieId": "MovieId",
            "tag": "TagText",
        })

        bad_condition = (
            Q.check_nulls(df, ["UserId", "MovieId"])
            | Q.check_epoch_range("timestamp")
        )
        good, bad = Q.split_good_and_bad(df, bad_condition, "FAILED_VALIDATION")

        good = T.epoch_to_timestamp(good, "timestamp", "TagTstmp")
        good = T.add_audit_columns(good)
        good = T.select_columns(good, [
            "UserId", "MovieId", "TagText", "TagTstmp",
            "CreateDtTm", "UpdateDtTm",
        ])

        good = apply_schema(good, TAGS_SCHEMA)
        
        # UPSERT instead of OVERWRITE
        upsert_via_staging(spark, good, "silver.tags", ["UserId", "MovieId", "TagText"])
        Q.write_quarantine(bad, "tags", mode="append")

        print(f"  batch {batch_num}: users {user_from:,} to {user_to:,}")
        user_from = user_to

    print(f"  done, {batch_num} batches")

def read_silver(spark, table):
    return spark.read.jdbc(
        url=JDBC_URL,
        table=f"silver.{table}",
        properties=JDBC_PROPS,
    )


def build_movie_metadata(spark):
    print("\n--- silver.movie_metadata ---")

    movies = read_silver(spark, "movies")
    links = read_silver(spark, "links")

    links = links.select("MovieId", "ImdbId", "TmdbId")
    df = movies.join(links, on="MovieId", how="left")
    df = df.drop("Title").withColumnRenamed("CleanTitle" , "Title")

    df = T.add_audit_columns(df)
    df = T.select_columns(df, [
        "MovieId", "Title", "ReleaseYear", "Genres",
        "ImdbId", "TmdbId", "CreateDtTm", "UpdateDtTm",
    ])

    upsert_via_staging(spark, df, "silver.movie_metadata", ["MovieId"])
    print("  done")

def build_user_ratings_master(spark, batch_size=20000):
    print("\n--- silver.user_ratings_master ---")

    metadata = read_silver(spark, "movie_metadata").select(
        "MovieId", "Title", "ReleaseYear", "Genres", "ImdbId", "TmdbId"
    )
    metadata.cache()

    batch_num = 0
    user_from = USER_ID_MIN

    while user_from <= USER_ID_MAX:
        user_to = user_from + batch_size
        batch_num += 1

        ratings = read_silver_user_range(spark, "ratings", user_from, user_to)
        ratings = ratings.select(
            "UserId", "MovieId", "Rating", "RatingTstmp"
        )

        tags = read_silver_user_range(spark, "tags", user_from, user_to)
        tags = T.aggregate_to_string(
            tags,
            group_cols=["UserId", "MovieId"],
            value_col="TagText",
            target_col="Tags",
            separator="|",
        )

        df = ratings.join(metadata, on="MovieId", how="left")
        df = df.join(tags, on=["UserId", "MovieId"], how="left")

        df = T.add_audit_columns(df)
        df = T.select_columns(df, [
            "UserId", "MovieId", "Title", "ReleaseYear", "Genres",
            "Tags", "Rating", "RatingTstmp", "ImdbId", "TmdbId",
            "CreateDtTm", "UpdateDtTm",
        ])

        upsert_via_staging(spark, df, "silver.user_ratings_master", ["UserId", "MovieId"])

        print(f"  batch {batch_num}: users {user_from:,} to {user_to:,}")
        user_from = user_to

    metadata.unpersist()
    print(f"  done, {batch_num} batches")


def report():
    print("\n" + "=" * 52)
    print(f"{'TABLE':<22}{'CLEAN':>13}{'QUARANTINED':>15}")
    print("-" * 52)

    total_bad = 0
    for table in ["links", "movies", "ratings", "tags"]:
        clean = count_rows("silver", table)
        bad = count_rows("quarantine", table)
        total_bad += bad
        print(f"silver.{table:<15}{clean:>13,}{bad:>15,}")

    print("-" * 52)
    print(f"{'TOTAL QUARANTINED':<22}{'':>13}{total_bad:>15,}")


def main():
    execute("CREATE SCHEMA IF NOT EXISTS quarantine;")

    spark = get_spark("build_silver")
    spark.conf.set("spark.sql.shuffle.partitions", "16")

    try:
        build_links(spark)
        build_movies(spark)
        build_ratings(spark)
        build_tags(spark)
        build_movie_metadata(spark)
        build_user_ratings_master(spark)
    finally:
        spark.stop()

    report()

if __name__ == "__main__":
    main()