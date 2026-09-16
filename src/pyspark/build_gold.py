"""
Build the four gold insight tables.

"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))

from pyspark.sql import functions as F
from pyspark.sql.window import Window

import gold_helpers as G
from config import JDBC_URL, JDBC_PROPS
from db import count_rows, execute
from spark_utils import get_spark, write_to_postgres

USER_ID_MIN = 1
USER_ID_MAX = 200948

# The half-star scale. Spec asks for a count column per value.
RATING_BUCKETS = [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]


def read_master(spark, num_partitions=16):
    """
    Read user_ratings_master in parallel chunks by UserId.

    Without partitioning, Spark pulls 32 million rows through one
    connection and runs out of memory.
    """
    return spark.read.jdbc(
        url=JDBC_URL,
        table="silver.user_ratings_master",
        column="UserId",
        lowerBound=USER_ID_MIN,
        upperBound=USER_ID_MAX,
        numPartitions=num_partitions,
        properties=JDBC_PROPS,
    )


def read_silver(spark, table):
    """Read a small silver table in one go."""
    return spark.read.jdbc(
        url=JDBC_URL, table=f"silver.{table}", properties=JDBC_PROPS
    )


# gold.movie_insight

def build_movie_insight(spark):
    """
    One row per movie: rating stats plus tag stats.

    Rating stats come from user_ratings_master.
    Tag stats come from silver.tags, because master only has tags
    that sit alongside a rating - a user can tag without rating.
    """
    print("\n--- gold.movie_insight ---")

    master = read_master(spark).select(
        "MovieId", "Title", "ReleaseYear", "Rating"
    )

    # --- rating aggregates ---
    rating_stats = master.groupBy("MovieId").agg(
        F.first("Title").alias("MovieTitle"),
        F.first("ReleaseYear").alias("ReleaseYear"),
        F.round(F.avg("Rating"), 4).alias("AvgRating"),
        F.max("Rating").alias("HighestRating"),
        F.min("Rating").alias("LowestRating"),
        F.count("*").alias("TotalRatings"),
    )

    # --- tag aggregates, from the raw tags table ---
    tags = read_silver(spark, "tags").select("UserId", "MovieId", "TagText")

    tag_stats = tags.groupBy("MovieId").agg(
        F.countDistinct("UserId").alias("TotalTaggers"),
        F.count("*").alias("TotalTags"),
        F.countDistinct("TagText").alias("DistinctTags"),
    )

    popular_tag = G.mode_of(tags, ["MovieId"], "TagText", "PopularTag")

    # --- combine ---
    df = (
        rating_stats
        .join(tag_stats, on="MovieId", how="left")
        .join(popular_tag, on="MovieId", how="left")
    )

    # Movies with no tags get null counts. Zero is the honest value.
    df = df.fillna({"TotalTaggers": 0, "TotalTags": 0, "DistinctTags": 0})

    df = G.add_gold_audit(df)
    df = df.select(
        "MovieId", "ReleaseYear", "MovieTitle", "AvgRating",
        "HighestRating", "LowestRating", "TotalRatings",
        "TotalTaggers", "TotalTags", "DistinctTags", "PopularTag",
        "LoadTs", "UpdateTs",
    )

    write_to_postgres(df, "gold.movie_insight", mode="overwrite")
    print("  done")


# gold.user_insight

def build_user_insight(spark):
    """
    One row per user: rating behaviour, genre preferences, recency.

    The genre parts need genres exploded, which multiplies rows
    roughly 2.5x. Done on a narrow selection to keep it manageable.
    """
    print("\n--- gold.user_insight ---")

    master = read_master(spark).select(
        "UserId", "MovieId", "Title", "Genres", "Rating", "RatingTstmp"
    )

    # --- basic rating stats ---
    base = master.groupBy("UserId").agg(
        F.countDistinct("MovieId").alias("MoviesRated"),
        F.round(F.avg("Rating"), 4).alias("AvgRatingLifetime"),
        F.min("Rating").alias("LeastRatingGiven"),
        F.max("Rating").alias("HighestRatingGiven"),
        F.sum(F.when(F.col("Rating") == 5.0, 1).otherwise(0))
            .alias("FiveStarCount"),
        F.countDistinct(F.year("RatingTstmp")).alias("_active_years"),
        F.count("*").alias("_total_ratings"),
    )

    # Average ratings per year = total ratings / number of years active.
    # Dividing by active years, not calendar years, is the honest
    # measure - a user who rated 100 films in one year then stopped
    # averaged 100 per year, not 100 divided by all years since.
    base = base.withColumn(
        "AvgRatingsPerYear",
        F.round(F.col("_total_ratings") / F.col("_active_years"), 2),
    ).drop("_active_years", "_total_ratings")

    # --- genre preferences ---
    exploded = master.withColumn(
        "Genre", F.explode(F.split(F.col("Genres"), r"\|"))
    ).select("UserId", "MovieId", "Genre", "Rating")

    genre_counts = exploded.groupBy("UserId", "Genre").agg(
        F.count("*").alias("_cnt"),
        F.avg("Rating").alias("_avg"),
    )

    # Top genre = the one they rated most often
    top_genre = G.top_row_per_group(
        genre_counts, ["UserId"],
        [F.col("_cnt").desc(), F.col("Genre").asc()],
        "Genre", "TopGenre",
    )

    # Best rated genre = the one they scored highest
    best_genre = G.top_row_per_group(
        genre_counts, ["UserId"],
        [F.col("_avg").desc(), F.col("Genre").asc()],
        "Genre", "BestRatedGenre",
    )

    # --- most recent activity ---
    recent_rated = G.top_row_per_group(
        master, ["UserId"],
        [F.col("RatingTstmp").desc()],
        "Title", "MostRecentRatedMovie",
    )

    # --- tag stats, from the raw tags table ---
    tags = read_silver(spark, "tags").select(
        "UserId", "MovieId", "TagText", "TagTstmp"
    )

    tag_stats = tags.groupBy("UserId").agg(
        F.count("*").alias("TotalTagsUsed"),
    )

    most_used_tag = G.mode_of(tags, ["UserId"], "TagText", "MostUsedTag")

    movies = read_silver(spark, "movies").select("MovieId", "Title")
    tags_with_title = tags.join(movies, on="MovieId", how="left")

    recent_tagged = G.top_row_per_group(
        tags_with_title, ["UserId"],
        [F.col("TagTstmp").desc()],
        "Title", "MostRecentTaggedMovie",
    )

    # --- combine ---
    df = (
        base
        .join(top_genre, on="UserId", how="left")
        .join(best_genre, on="UserId", how="left")
        .join(recent_rated, on="UserId", how="left")
        .join(tag_stats, on="UserId", how="left")
        .join(most_used_tag, on="UserId", how="left")
        .join(recent_tagged, on="UserId", how="left")
    )

    df = df.fillna({"TotalTagsUsed": 0})

    df = G.add_gold_audit(df)
    df = df.select(
        "UserId", "MoviesRated", "AvgRatingLifetime", "AvgRatingsPerYear",
        "LeastRatingGiven", "HighestRatingGiven", "FiveStarCount",
        "TotalTagsUsed", "MostUsedTag", "TopGenre", "BestRatedGenre",
        "MostRecentRatedMovie", "MostRecentTaggedMovie",
        "LoadTs", "UpdateTs",
    )

    write_to_postgres(df, "gold.user_insight", mode="overwrite")
    print("  done")

# gold.genre_insight

def build_genre_insight(spark):
    """
    One row per genre. The hardest table in the project.

    Needs: bucketed rating counts, several "which movie is X" answers,
    and two arrays of ten movie titles each.

    Only ~20 output rows, but the input is 32M rows exploded by genre.
    """
    print("\n--- gold.genre_insight ---")

    master = read_master(spark).select(
        "MovieId", "Title", "ReleaseYear", "Genres", "Rating"
    )

    exploded = master.withColumn(
        "Genre", F.explode(F.split(F.col("Genres"), r"\|"))
    ).select("Genre", "MovieId", "Title", "ReleaseYear", "Rating")

    # Cache: we scan this several times below.
    exploded.cache()

    # --- top level aggregates, including rating buckets ---
    bucket_aggs = [
        F.sum(F.when(F.col("Rating") == b, 1).otherwise(0))
            .alias(f"Rated{str(b).replace('.', '_')}")
        for b in RATING_BUCKETS
    ]

    base = exploded.groupBy("Genre").agg(
        F.countDistinct("MovieId").alias("MovieCount"),
        F.count("*").alias("TotalRatings"),
        F.round(F.avg("Rating"), 4).alias("AvgRating"),
        *bucket_aggs,
    )

    # --- per-movie stats inside each genre ---
    per_movie = exploded.groupBy("Genre", "MovieId").agg(
        F.first("Title").alias("Title"),
        F.first("ReleaseYear").alias("ReleaseYear"),
        F.count("*").alias("RatingCount"),
        F.avg("Rating").alias("MovieAvg"),
    )
    per_movie.cache()

    # Instead of calling top_row_per_group six times (which shuffles
    # the whole table six separate times), we compute all four rankings
    # in ONE pass, then just filter the same result four different ways.
    w_most  = Window.partitionBy("Genre").orderBy(F.col("RatingCount").desc(), F.col("Title").asc())
    w_least = Window.partitionBy("Genre").orderBy(F.col("RatingCount").asc(),  F.col("Title").asc())
    w_best  = Window.partitionBy("Genre").orderBy(F.col("MovieAvg").desc(),    F.col("Title").asc())
    w_worst = Window.partitionBy("Genre").orderBy(F.col("MovieAvg").asc(),     F.col("ReleaseYear").desc())

    ranked = (
        per_movie
        .withColumn("_rn_most",  F.row_number().over(w_most))
        .withColumn("_rn_least", F.row_number().over(w_least))
        .withColumn("_rn_best",  F.row_number().over(w_best))
        .withColumn("_rn_worst", F.row_number().over(w_worst))
    )
    ranked = ranked.cache()
    ranked.count()   # forces Spark to compute and store this now

    most_rated = ranked.filter(F.col("_rn_most") == 1) \
        .select("Genre", F.col("Title").alias("MostRatedMovie"))

    least_rated = ranked.filter(F.col("_rn_least") == 1) \
        .select("Genre", F.col("Title").alias("LeastRatedMovie"))

    highest_avg = ranked.filter((F.col("RatingCount") >= 100) & (F.col("_rn_best") == 1)) \
        .select("Genre", F.col("Title").alias("HighestAvgRatedMovie"))

    worst = ranked.filter(F.col("_rn_worst") == 1) \
        .select("Genre", F.col("Title").alias("WorstRatedMovie"))

    top10_popular = ranked.filter(F.col("_rn_most") <= 10) \
        .groupBy("Genre").agg(F.collect_list("Title").alias("Top10PopularMovies"))

    top10_rated = ranked.filter((F.col("RatingCount") >= 100) & (F.col("_rn_best") <= 10)) \
        .groupBy("Genre").agg(F.collect_list("Title").alias("Top10RatedMovies"))

    # --- popular tag per genre ---
    tags = read_silver(spark, "tags").select("MovieId", "TagText")
    movie_genres = read_silver(spark, "movies").select("MovieId", "Genres")
    movie_genres = movie_genres.withColumn(
        "Genre", F.explode(F.split(F.col("Genres"), r"\|"))
    ).select("MovieId", "Genre")

    tags_by_genre = tags.join(movie_genres, on="MovieId", how="inner")
    popular_tag = G.mode_of(tags_by_genre, ["Genre"], "TagText", "PopularTag")

    # --- combine ---
    df = (
        base
        .join(popular_tag, on="Genre", how="left")
        .join(most_rated, on="Genre", how="left")
        .join(least_rated, on="Genre", how="left")
        .join(highest_avg, on="Genre", how="left")
        .join(worst, on="Genre", how="left")
        .join(top10_popular, on="Genre", how="left")
        .join(top10_rated, on="Genre", how="left")
    )

    df = G.add_gold_audit(df)

    write_to_postgres(df, "gold.genre_insight", mode="overwrite")

    exploded.unpersist()
    per_movie.unpersist()
    ranked.unpersist()
    print("  done")


# gold.yearly_insight

def build_yearly_insight(spark):
    """
    One row per release year.

    Note: "year" here means the movie's RELEASE year, not the year
    the rating was given. That matches the spec's "number of movies
    released in the year".
    """
    print("\n--- gold.yearly_insight ---")

    master = read_master(spark).select(
        "MovieId", "Title", "ReleaseYear", "Genres", "Rating"
    )

    # Movies with no year in the title cannot be attributed
    master = master.filter(F.col("ReleaseYear").isNotNull())
    master.cache()

    base = master.groupBy("ReleaseYear").agg(
        F.countDistinct("MovieId").alias("MoviesReleased"),
        F.count("*").alias("TotalRatingsGiven"),
    )

    per_movie = master.groupBy("ReleaseYear", "MovieId").agg(
        F.first("Title").alias("Title"),
        F.count("*").alias("RatingCount"),
        F.avg("Rating").alias("MovieAvg"),
        F.min("Rating").alias("MovieMin"),
    )
    per_movie.cache()

    most_popular = G.top_row_per_group(
        per_movie, ["ReleaseYear"],
        [F.col("RatingCount").desc(), F.col("Title").asc()],
        "Title", "MostPopularMovie",
    )

    highly_rated = G.top_row_per_group(
        per_movie.filter(F.col("RatingCount") >= 100), ["ReleaseYear"],
        [F.col("MovieAvg").desc(), F.col("Title").asc()],
        "Title", "HighlyRatedMovie",
    )

    highest_avg = per_movie.groupBy("ReleaseYear").agg(
        F.round(F.max("MovieAvg"), 4).alias("HighestAvgRating"),
        F.min("MovieMin").alias("LowestRatingGiven"),
    )

    worst = G.top_row_per_group(
        per_movie, ["ReleaseYear"],
        [F.col("MovieAvg").asc(), F.col("Title").asc()],
        "Title", "WorstRatedMovie",
    )

    # --- genre stats per year ---
    exploded = master.withColumn(
        "Genre", F.explode(F.split(F.col("Genres"), r"\|"))
    ).select("ReleaseYear", "Genre", "Rating")

    genre_avg = exploded.groupBy("ReleaseYear", "Genre").agg(
        F.avg("Rating").alias("_avg")
    )

    top_genre = G.top_row_per_group(
        genre_avg, ["ReleaseYear"],
        [F.col("_avg").desc(), F.col("Genre").asc()],
        "Genre", "TopRatedGenre",
    )

    least_genre = G.top_row_per_group(
        genre_avg, ["ReleaseYear"],
        [F.col("_avg").asc(), F.col("Genre").asc()],
        "Genre", "LeastRatedGenre",
    )

    # --- tag stats per year ---
    tags = read_silver(spark, "tags").select("MovieId", "TagText")
    movie_years = read_silver(spark, "movie_metadata").select(
        "MovieId", "ReleaseYear"
    )
    tags_by_year = tags.join(movie_years, on="MovieId", how="inner").filter(
        F.col("ReleaseYear").isNotNull()
    )

    tag_counts = tags_by_year.groupBy("ReleaseYear").agg(
        F.countDistinct("TagText").alias("DistinctTagsUsed")
    )

    popular_tag = G.mode_of(
        tags_by_year, ["ReleaseYear"], "TagText", "MostPopularTag"
    )

    # --- combine ---
    df = (
        base
        .join(most_popular, on="ReleaseYear", how="left")
        .join(highly_rated, on="ReleaseYear", how="left")
        .join(highest_avg, on="ReleaseYear", how="left")
        .join(worst, on="ReleaseYear", how="left")
        .join(top_genre, on="ReleaseYear", how="left")
        .join(least_genre, on="ReleaseYear", how="left")
        .join(tag_counts, on="ReleaseYear", how="left")
        .join(popular_tag, on="ReleaseYear", how="left")
    )

    df = df.fillna({"DistinctTagsUsed": 0})
    df = G.add_gold_audit(df)
    df = df.withColumnRenamed("ReleaseYear", "Year")

    write_to_postgres(df, "gold.yearly_insight", mode="overwrite")

    master.unpersist()
    per_movie.unpersist()
    print("  done")




def report():
    print("\n" + "=" * 46)
    print(f"{'GOLD TABLE':<28}{'ROWS':>16}")
    print("-" * 46)
    for table in ["movie_insight", "user_insight",
                  "genre_insight", "yearly_insight"]:
        print(f"gold.{table:<23}{count_rows('gold', table):>16,}")


def main():
    execute("CREATE SCHEMA IF NOT EXISTS gold;")

    spark = get_spark("build_gold")

    # Gold does heavy shuffles. More partitions means smaller chunks,
    # which is what keeps 4 GB workable.
    spark.conf.set("spark.sql.shuffle.partitions", "64")

    try:
        build_movie_insight(spark)
        build_user_insight(spark)
        build_genre_insight(spark)
        build_yearly_insight(spark)
    finally:
        spark.stop()

    report()


if __name__ == "__main__":
    main()