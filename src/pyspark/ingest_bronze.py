"""
Load raw CSVs into the bronze schema.
"""

import sys
from pathlib import Path

# Let this file import from src/python
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))

import audit
from config import RAW_DIR
from db import count_rows
from spark_utils import get_spark, add_load_timestamp, write_to_postgres


def read_bronze_csv(spark, path):
    """Read a raw CSV with Spark schema inference enabled."""
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .csv(str(path))
    )


# single file

def load_single_file(spark, source_name, file_name):
    """
    Load one CSV into bronze.<source_name>, overwriting what's there.

    Used for links, movies and tags. These are small single files, so
    overwriting is fast and always safe.
    """
    print(f"\n--- {source_name} ---")

    path = RAW_DIR / file_name

    if not path.exists():
        print(f"MISSING: {path}")
        audit.log_load(source_name, file_name, 0, None, "MISSING_FILE")
        return

    df = read_bronze_csv(spark, path)

    df = add_load_timestamp(df)
    rows = write_to_postgres(
        df,
        f"bronze.{source_name}",
        mode="overwrite",
    )

    print(f"Loaded {rows:,} rows into bronze.{source_name}")
    audit.log_load(
        source_name,
        file_name,
        rows,
        "overwrite",
        "SUCCESS",
    )


# ratings

def load_ratings(spark):
    """
    Load the ratings files one at a time.

    First file overwrites, the rest append. Any file already recorded as
    SUCCESS in the audit log is skipped, so re-running is safe.
    """
    print("\n--- ratings ---")

    # Find all ratings_part*.csv files, sorted so part1 comes first
    files = sorted(RAW_DIR.glob("ratings_part*.csv"))

    if not files:
        print("No ratings files found")
        audit.log_load("ratings", None, 0, None, "MISSING_FILE")
        return

    print(f"Found {len(files)} files: {[f.name for f in files]}")

    already_loaded = audit.get_loaded_files("ratings")
    if already_loaded:
        print(f"Already loaded: {sorted(already_loaded)}")

    for index, path in enumerate(files):

        if path.name in already_loaded:
            print(f"SKIP {path.name}")
            continue

        # Overwrite only if this is the first file AND nothing is loaded yet.
        # If some parts are already in the table, we must append, not wipe.
        if index == 0 and not already_loaded:
            mode = "overwrite"
        else:
            mode = "append"

        df = read_bronze_csv(spark, path)

        df = add_load_timestamp(df)
        rows = write_to_postgres(
            df,
            "bronze.ratings",
            mode=mode,
        )

        print(f"{path.name} -> {rows:,} rows ({mode})")
        audit.log_load(
            "ratings",
            path.name,
            rows,
            mode,
            "SUCCESS",
        )


# main

def main():
    audit.create_log_table()

    spark = get_spark("bronze_ingestion")

    try:
        load_single_file(spark, "links", "links.csv")
        load_single_file(spark, "movies", "movies.csv")
        load_single_file(spark, "tags", "tags.csv")
        load_ratings(spark)
    finally:
        spark.stop()

    print("\n" + "=" * 50)
    print("FINAL ROW COUNTS")
    for table in ["links", "movies", "tags", "ratings"]:
        print(
            f"  bronze.{table:<8} "
            f"{count_rows('bronze', table):,}"
        )


if __name__ == "__main__":
    main()