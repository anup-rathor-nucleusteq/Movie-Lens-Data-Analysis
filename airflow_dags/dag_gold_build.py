"""
Gold layer DAG.

Runs automatically when silver_transform finishes, via the
SILVER_READY dataset.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from datasets import SILVER_READY


def _run_build(which):
    from spark_utils import get_spark
    import build_gold as job

    spark = get_spark(f"airflow_gold_{which}")
    spark.conf.set("spark.sql.shuffle.partitions", "64")

    try:
        if which == "movie_insight":
            job.build_movie_insight(spark)
        elif which == "user_insight":
            job.build_user_insight(spark)
        elif which == "genre_insight":
            job.build_genre_insight(spark)
        elif which == "yearly_insight":
            job.build_yearly_insight(spark)
        else:
            raise ValueError(f"Unknown build: {which}")
    finally:
        spark.stop()


def create_gold_schema():
    from db import execute
    execute("CREATE SCHEMA IF NOT EXISTS gold;")
    print("gold schema ready")


def verify_gold():
    """Every gold table must have rows. An empty one means a silent failure."""
    from db import count_rows

    expected_min = {
        "movie_insight": 80000,
        "user_insight": 190000,
        "genre_insight": 15,
        "yearly_insight": 100,
    }

    problems = []
    for table, minimum in expected_min.items():
        rows = count_rows("gold", table)
        print(f"gold.{table:<20} {rows:>12,}")
        if rows < minimum:
            problems.append(f"{table}: only {rows:,} rows, expected {minimum:,}+")

    if problems:
        raise ValueError("Gold verification failed -> " + "; ".join(problems))

    print("\nAll gold tables populated.")


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="gold_build",
    description="Build the four gold insight tables",
    start_date=datetime(2026, 9, 1),
    schedule=[SILVER_READY],
    catchup=False,
    default_args=default_args,
    tags=["movielens", "gold"],
) as dag:

    setup = PythonOperator(
        task_id="create_gold_schema",
        python_callable=create_gold_schema,
    )

    movie = PythonOperator(
        task_id="gold_movie_insight",
        python_callable=_run_build,
        op_args=["movie_insight"],
    )

    user = PythonOperator(
        task_id="gold_user_insight",
        python_callable=_run_build,
        op_args=["user_insight"],
    )

    genre = PythonOperator(
        task_id="gold_genre_insight",
        python_callable=_run_build,
        op_args=["genre_insight"],
    )

    yearly = PythonOperator(
        task_id="gold_yearly_insight",
        python_callable=_run_build,
        op_args=["yearly_insight"],
    )

    verify = PythonOperator(
        task_id="verify_gold",
        python_callable=verify_gold,
    )

    setup >> movie >> user >> genre >> yearly >> verify