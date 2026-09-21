"""
Load raw CSVs into the bronze schema.
"""

import sys
from pathlib import Path

# Let this file import from src/python
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))

import audit
from config import RAW_DIR
from db import count_rows , execute
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
    Load one CSV into bronze.<source_name>.
    Simulates real-world append by truncating first for local hardware limits.
    """
    print(f"\n--- {source_name} ---")
    path = RAW_DIR / file_name

    if not path.exists():
        print(f"MISSING: {path}")
        audit.log_load(source_name, file_name, 0, None, "MISSING_FILE")
        return

    df = read_bronze_csv(spark, path)
    df = add_load_timestamp(df)

    # ---FIX: Delete the data before appending ---
    # We truncate the table and clear its audit history so we get a fresh start
    execute(f"TRUNCATE TABLE bronze.{source_name};")
    execute(f"DELETE FROM audit.ingestion_log WHERE source_name = '{source_name}';")
    # Now we can safely use append
    rows = write_to_postgres(
        df,
        f"bronze.{source_name}",
        mode="append", 
    )

    print(f"Loaded {rows:,} rows into bronze.{source_name}")
    audit.log_load(source_name, file_name, rows, "append", "SUCCESS")


# ratings

def load_ratings(spark):
    print("\n--- ratings ---")
    files = sorted(RAW_DIR.glob("ratings_part*.csv"))

    if not files:
        print("No ratings files found")
        audit.log_load("ratings", None, 0, None, "MISSING_FILE")
        return

    # ---FIX: Reset the table before the batch starts ---
    # We wipe the entire 32 million rows and the audit log ONCE at the start.
    print("Truncating bronze.ratings to prepare for fresh append...")
    execute("TRUNCATE TABLE bronze.ratings;")
    execute("DELETE FROM audit.ingestion_log WHERE source_name = 'ratings';")

    # Because we cleared the log, already_loaded will be empty, 
    # but we still fetch it just in case your audit logic requires it.
    already_loaded = audit.get_loaded_files("ratings")

    for index, path in enumerate(files):
        if path.name in already_loaded:
            print(f"SKIP {path.name}")
            continue

        df = read_bronze_csv(spark, path)
        df = add_load_timestamp(df)

        # Now EVERY file uses append, simulating a real-world streaming/batch process
        mode = "append"
        
        rows = write_to_postgres(
            df,
            "bronze.ratings",
            mode=mode,
        )

        print(f"{path.name} -> {rows:,} rows ({mode})")
        audit.log_load("ratings", path.name, rows, mode, "SUCCESS")


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