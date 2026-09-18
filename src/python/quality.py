"""
Data quality checks for the silver layer.
  - Good rows go to silver, bad rows go to quarantine.
"""

from pyspark.sql import functions as F

from db import execute
from pyspark.sql import functions as F
from spark_utils import write_to_postgres


# Valid ranges from the MovieLens documentation
RATING_MIN = 0.5
RATING_MAX = 5.0
EPOCH_MIN = 789652000   # 1995-01-09, first rating in the dataset
EPOCH_MAX = 1697241600   # 2023-10-13, dataset generation date


def check_nulls(df, columns):
    """
    Returns a condition that is TRUE when any of these columns is null.

    F.lit(False) means "start with false".
    Then for each column we do: condition = condition OR (column is null)

    So if ANY column is null, the whole thing becomes true.
    """
    condition = F.lit(False)
    for col in columns:
        condition = condition | F.col(col).isNull()
    return condition


def check_rating_range(rating_col="Rating"):
    """TRUE when the rating is outside 0.5 to 5.0."""
    return (F.col(rating_col) < RATING_MIN) | (F.col(rating_col) > RATING_MAX)


def check_epoch_range(epoch_col="timestamp"):
    """TRUE when the timestamp is outside the dataset's date range."""
    return (F.col(epoch_col) < EPOCH_MIN) | (F.col(epoch_col) > EPOCH_MAX)


def split_good_and_bad(df, bad_condition, reason):
    """
    Split one DataFrame into two: good rows and bad rows.

    Returns (good_df, bad_df).
    The bad rows get an extra column saying why they were rejected.

     we use ~ (tilde) to flip the condition.
    ~bad_condition means "not bad", which is "good".
    """
    good_df = df.filter(~bad_condition)
    bad_df = df.filter(bad_condition).withColumn(
        "RejectReason", F.lit(reason)
    )
    return good_df, bad_df


def create_quarantine_table(table_name):
    """
    Make an empty quarantine table if it doesn't exist.

    We let Spark create the actual columns when it writes, so this
    just makes sure the schema exists.
    """
    execute(f"CREATE SCHEMA IF NOT EXISTS quarantine;")


def write_quarantine(bad_df, table_name , mode="overwrite"):
    """
    Write bad rows to quarantine.<table_name>.

    The count is read back from Postgres later, which is instant.
    """

    bad_df = bad_df.withColumn("QuarantinedAt", F.current_timestamp())
    write_to_postgres(bad_df, f"quarantine.{table_name}", mode=mode)
    print(f"  quarantine.{table_name} written")