"""
Silver transformation DAG.
"""

from datetime import datetime, timedelta
from datasets import SILVER_READY , BRONZE_READY
from airflow import DAG
from airflow.operators.python import PythonOperator
from db import count_rows
from spark_utils import get_spark
import build_silver as job
from db import execute


def _run_build(which):
    """
    Start Spark, run one build function, stop Spark.
    """

    spark = get_spark(f"airflow_silver_{which}")
    spark.conf.set("spark.sql.shuffle.partitions", "16")

    try:
        if which == "links":
            job.build_links(spark)
        elif which == "movies":
            job.build_movies(spark)
        elif which == "ratings":
            job.build_ratings(spark)
        elif which == "tags":
            job.build_tags(spark)
        elif which == "movie_metadata":
            job.build_movie_metadata(spark)
        elif which == "user_ratings_master":
            job.build_user_ratings_master(spark)
        else:
            raise ValueError(f"Unknown build: {which}")
    finally:
        spark.stop()


def create_quarantine_schema():
    """Make sure the quarantine schema exists before anything writes to it."""
    execute("CREATE SCHEMA IF NOT EXISTS quarantine;")
    print("quarantine schema ready")


def verify_silver():
    """
    Check silver is correct.

    The four base tables came from bronze, so silver + quarantine
    must equal bronze. The two derived tables were built by joining
    silver tables together - there is no bronze equivalent, so we
    just confirm they have rows.
    """

    problems = []

    # --- tables with a bronze source ---
    for table in ["links", "movies", "ratings", "tags"]:
        bronze = count_rows("bronze", table)
        silver = count_rows("silver", table)
        bad = count_rows("quarantine", table)

        print(f"{table:<10} bronze={bronze:>12,}  "
              f"silver={silver:>12,}  quarantined={bad:>6,}")

        if silver + bad != bronze:
            problems.append(
                f"{table}: silver {silver:,} + quarantine {bad:,} "
                f"does not equal bronze {bronze:,}"
            )

    # --- derived tables: just confirm they are not empty ---
    for table, minimum in [("movie_metadata", 80000),
                            ("user_ratings_master", 30000000)]:
        rows = count_rows("silver", table)
        print(f"{table:<20} rows={rows:>12,}")
        if rows < minimum:
            problems.append(f"{table}: only {rows:,} rows, expected {minimum:,}+")

    if problems:
        raise ValueError("Row count mismatch -> " + "; ".join(problems))

    print("\nAll rows accounted for. Silver layer verified.")


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="silver_transform",
    description="Clean bronze tables into the silver layer",
    start_date=datetime(2026, 9, 1),
    schedule=[BRONZE_READY],
    catchup=False,
    default_args=default_args,
    tags=["movielens", "silver"],
) as dag:

    setup = PythonOperator(
        task_id="create_quarantine_schema",
        python_callable=create_quarantine_schema,
    )

    links = PythonOperator(
        task_id="silver_links",
        python_callable=_run_build,
        op_args=["links"],
    )

    movies = PythonOperator(
        task_id="silver_movies",
        python_callable=_run_build,
        op_args=["movies"],
    )

    ratings = PythonOperator(
        task_id="silver_ratings",
        python_callable=_run_build,
        op_args=["ratings"],
    )

    tags = PythonOperator(
        task_id="silver_tags",
        python_callable=_run_build,
        op_args=["tags"],
    )

    metadata = PythonOperator(
        task_id="silver_movie_metadata",
        python_callable=_run_build,
        op_args=["movie_metadata"],
    )
    master = PythonOperator(
        task_id = "silver_user_ratings_master",
        python_callable=_run_build,
        op_args=["user_ratings_master",]
    )

    verify = PythonOperator(
        task_id="verify_silver",
        python_callable=verify_silver,
        outlets=[SILVER_READY],
    )

    # links and movies run together - small and independent.
    # ratings runs alone so it gets all the memory.
    # tags follows ratings, then verification.
    setup >> [links, movies] >> ratings >> tags >> metadata >> master >> verify