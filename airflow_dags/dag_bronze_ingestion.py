"""
Bronze ingestion DAG.
"""

from datetime import datetime, timedelta
from datasets import BRONZE_READY
from airflow import DAG
from airflow.operators.python import PythonOperator
from db import count_rows
from spark_utils import get_spark
import ingest_bronze as job
import audit


def _run_with_spark(loader_name):
    """
    Start Spark, run one loader function, stop Spark.
    """
    
    spark = get_spark(f"airflow_{loader_name}")

    try:
        if loader_name == "links":
            job.load_single_file(
                spark,
                "links",
                "links.csv",
            )

        elif loader_name == "movies":
            job.load_single_file(
                spark,
                "movies",
                "movies.csv",
            )

        elif loader_name == "tags":
            job.load_single_file(
                spark,
                "tags",
                "tags.csv",
            )

        elif loader_name == "ratings":
            job.load_ratings(spark)

        else:
            raise ValueError(f"Unknown loader: {loader_name}")

    finally:
        spark.stop()


def create_audit_table():
    """Make sure audit.ingestion_log exists before anything else runs."""
    audit.create_log_table()


def verify_counts():
    """Print final row counts. Fails the task if anything is empty."""

    expected = {
        "links": 87585,
        "movies": 87585,
        "tags": 2000072,
        "ratings": 32000204,
    }

    problems = []

    for table, want in expected.items():
        got = count_rows("bronze", table)

        print(
            f"bronze.{table:<8} "
            f"{got:>12,}  "
            f"(expected {want:,})"
        )

        if got != want:
            problems.append(
                f"{table}: got {got:,}, expected {want:,}"
            )

    if problems:
        raise ValueError(
            "Row count mismatch -> " + "; ".join(problems)
        )

    print("All row counts match the official dataset.")


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="bronze_ingestion",
    description="Load MovieLens CSVs into the bronze schema",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["movielens", "bronze"],
) as dag:

    setup = PythonOperator(
        task_id="create_audit_table",
        python_callable=create_audit_table,
    )

    load_links = PythonOperator(
        task_id="load_links",
        python_callable=_run_with_spark,
        op_args=["links"],
    )

    load_movies = PythonOperator(
        task_id="load_movies",
        python_callable=_run_with_spark,
        op_args=["movies"],
    )

    load_tags = PythonOperator(
        task_id="load_tags",
        python_callable=_run_with_spark,
        op_args=["tags"],
    )

    load_ratings = PythonOperator(
        task_id="load_ratings",
        python_callable=_run_with_spark,
        op_args=["ratings"],
    )

    verify = PythonOperator(
        task_id="verify_row_counts",
        python_callable=verify_counts,
        outlets=[BRONZE_READY],
    )

    # setup runs first, then the three small loads in PARALLEL,
    # then ratings, then verification.
    setup >> [load_links, load_movies, load_tags] >> load_ratings >> verify