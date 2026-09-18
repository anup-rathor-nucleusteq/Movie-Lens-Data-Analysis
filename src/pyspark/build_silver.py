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

    Spark splits the work by ranges of userId. With 8 partitions,
    connection 1 fetches roughly userId 1-25000, connection 2 fetches
    25001-50000, and so on.

    This is the fix for the OutOfMemoryError. Without it, Spark pulls
    all 32 million rows through ONE connection into ONE partition,
    and 4 GB cannot hold that.
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
# Bronze is inferred; Silver owns the final data types.
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
    write_to_postgres(good, "silver.links", mode="overwrite")
    Q.write_quarantine(bad, "links")



# silver.movies


def build_movies(spark):
    print("\n--- silver.movies ---")

    df = read_small(spark, "movies")

    # Extract the year while the column is still called "title"
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
    write_to_postgres(good, "silver.movies", mode="overwrite")
    Q.write_quarantine(bad, "movies")


def read_user_range(spark, table, user_from, user_to):
    """Read only rows for a range of userIds. The WHERE runs in Postgres."""
    query = (
        f'(SELECT * FROM bronze.{table} '
        f'WHERE "userId" >= {user_from} AND "userId" < {user_to}) AS chunk'
    )
    return spark.read.jdbc(url=JDBC_URL, table=query, properties=JDBC_PROPS)

def read_silver_user_range(spark, table, user_from, user_to):
    """
    Same as read_user_range but reads from silver, not bronze.

    Note the column is "UserId" here, not "userId" - silver uses
    PascalCase. Getting this wrong gives a column-not-found error.
    """
    query = (
        f'(SELECT * FROM silver.{table} '
        f'WHERE "UserId" >= {user_from} AND "UserId" < {user_to}) AS chunk'
    )
    return spark.read.jdbc(url=JDBC_URL, table=query, properties=JDBC_PROPS)


def build_ratings(spark, batch_size=20000):
    """
    Build silver.ratings in batches of 20,000 users at a time.

    Roughly 3 million rows per batch, which fits in 4 GB.
    First batch overwrites, the rest append.
    """
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

        # Safe to dedupe within a batch: duplicates on (UserId, MovieId)
        # are always the same user, so always in the same batch.
        good = T.dedupe_keep_latest(good, ["UserId", "MovieId"], "timestamp")

        good = T.add_audit_columns(good)
        good = T.select_columns(good, [
            "UserId", "MovieId", "Rating", "RatingTstmp",
            "CreateDtTm", "UpdateDtTm",
        ])

        mode = "overwrite" if batch_num == 1 else "append"
        good = apply_schema(good, RATINGS_SCHEMA)
        write_to_postgres(good, "silver.ratings", mode=mode)
        Q.write_quarantine(bad, "ratings", mode)

        print(f"  batch {batch_num}: users {user_from:,} to {user_to:,}")
        user_from = user_to

    print(f"  done, {batch_num} batches")


def build_tags(spark, batch_size=50000):
    """
    Same batching. NO dedupe - a user tags one movie many times.
    """
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

        mode = "overwrite" if batch_num == 1 else "append"
        good = apply_schema(good, TAGS_SCHEMA)
        write_to_postgres(good, "silver.tags", mode=mode)
        Q.write_quarantine(bad, "tags", mode)

        print(f"  batch {batch_num}: users {user_from:,} to {user_to:,}")
        user_from = user_to

    print(f"  done, {batch_num} batches")

def read_silver(spark, table):
    """Read a silver table. Small ones only."""
    return spark.read.jdbc(
        url=JDBC_URL,
        table=f"silver.{table}",
        properties=JDBC_PROPS,
    )


def build_movie_metadata(spark):
    """
    Join silver.movies to silver.links.

    One row per movie with everything about that movie in it.
    87,585 rows, so no batching needed.
    """
    print("\n--- silver.movie_metadata ---")

    movies = read_silver(spark, "movies")
    links = read_silver(spark, "links")

    # Drop the audit columns from links before joining, otherwise both
    # sides have CreateDtTm and UpdateDtTm and Spark cannot tell them apart.
    links = links.select("MovieId", "ImdbId", "TmdbId")

    # LEFT join, not inner. If a movie has no link row we keep the movie
    # and leave the IDs null. An inner join would silently delete it.
    df = movies.join(links, on="MovieId", how="left")

    df = df.drop("Title").withColumnRenamed("CleanTitle" , "Title")

    df = T.add_audit_columns(df)
    df = T.select_columns(df, [
        "MovieId", "Title", "ReleaseYear", "Genres",
        "ImdbId", "TmdbId", "CreateDtTm", "UpdateDtTm",
    ])

    write_to_postgres(df, "silver.movie_metadata", mode="overwrite")
    print("  done")

def build_user_ratings_master(spark, batch_size=20000):
    """
    The big flat table. Every rating with movie details and tags attached.

    Built in batches of 20,000 users, same as build_ratings.
    Roughly 3 million rows per batch.

    """
    print("\n--- silver.user_ratings_master ---")

    # movie_metadata is only 87k rows, so read it once and reuse it
    # for every batch instead of re-reading 11 times.
    metadata = read_silver(spark, "movie_metadata").select(
        "MovieId", "Title", "ReleaseYear", "Genres", "ImdbId", "TmdbId"
    )
    metadata.cache()

    batch_num = 0
    user_from = USER_ID_MIN

    while user_from <= USER_ID_MAX:
        user_to = user_from + batch_size
        batch_num += 1

        # --- ratings for this batch of users ---
        ratings = read_silver_user_range(spark, "ratings", user_from, user_to)
        ratings = ratings.select(
            "UserId", "MovieId", "Rating", "RatingTstmp"
        )

        # --- tags for the same users, squashed to one row per pair ---
        tags = read_silver_user_range(spark, "tags", user_from, user_to)
        tags = T.aggregate_to_string(
            tags,
            group_cols=["UserId", "MovieId"],
            value_col="TagText",
            target_col="Tags",
            separator="|",
        )

        # --- join ---
        # LEFT joins throughout. A rating must survive even if the movie
        # has no metadata and the user left no tags.
        df = ratings.join(metadata, on="MovieId", how="left")
        df = df.join(tags, on=["UserId", "MovieId"], how="left")

        df = T.add_audit_columns(df)
        df = T.select_columns(df, [
            "UserId", "MovieId", "Title", "ReleaseYear", "Genres",
            "Tags", "Rating", "RatingTstmp", "ImdbId", "TmdbId",
            "CreateDtTm", "UpdateDtTm",
        ])

        mode = "overwrite" if batch_num == 1 else "append"
        write_to_postgres(df, "silver.user_ratings_master", mode=mode)

        print(f"  batch {batch_num}: users {user_from:,} to {user_to:,}")
        user_from = user_to

    metadata.unpersist()
    print(f"  done, {batch_num} batches")



# Fianl Report

def report():
    """Read the final counts back from Postgres. Instant, no Spark."""
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

    # More shuffle partitions = smaller chunks during the dedupe.
    # The default of 200 is too many for a laptop; 64 keeps each
    # chunk small enough for 4 GB while avoiding excessive overhead.
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