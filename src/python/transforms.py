"""
Column-level transformations for the silver layer.

"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Anchoring with $ matters. Without it, "1984 (1956)" would match
# 1984 and give the wrong year.
YEAR_PATTERN = r"\((\d{4})\)\s*$"


def extract_release_year(df: DataFrame, title_col: str = "title") -> DataFrame:
    """
    Add a ReleaseYear column by reading the year out of the title.

    Titles with no year get NULL. We do NOT drop those rows - they are
    real movies that simply lack the metadata.
    """
    return df.withColumn(
        "ReleaseYear",
        F.regexp_extract(F.col(title_col), YEAR_PATTERN, 1).cast("int"),
    )


def clean_title(df: DataFrame, title_col: str = "title") -> DataFrame:
    """
    Add a CleanTitle column with the year stripped off.

    "Toy Story (1995)" becomes "Toy Story".
    Useful later for matching titles against external sources.
    """
    return df.withColumn(
        "CleanTitle",
        F.trim(F.regexp_replace(F.col(title_col), YEAR_PATTERN, "")),
    )


def epoch_to_timestamp(df: DataFrame, source_col: str, target_col: str) -> DataFrame:
    """
    Turn epoch seconds (944249077) into a real timestamp.

    The session timezone is already pinned to UTC in spark_utils.py,
    so the result is UTC as the spec requires.
    """
    return df.withColumn(target_col, F.timestamp_seconds(F.col(source_col)))


def add_audit_columns(df: DataFrame) -> DataFrame:
    """
    Add CreateDtTm and UpdateDtTm, both set to now in UTC.

    """
    now = F.current_timestamp()
    return df.withColumn("CreateDtTm", now).withColumn("UpdateDtTm", now)


def rename_columns(df: DataFrame, mapping: dict) -> DataFrame:
    """
    Rename columns using a {old: new} dictionary.

    Used to convert bronze's camelCase to silver's PascalCase.
    Columns not in the mapping are left alone.
    """
    for old_name, new_name in mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df


def select_columns(df: DataFrame, columns: list) -> DataFrame:
    """
    Keep only these columns, in this order.

    """
    return df.select(*columns)


def dedupe_keep_latest(df: DataFrame, key_cols: list, order_col: str) -> DataFrame:
    """
    Remove duplicates on key_cols, keeping the row with the highest
    order_col value.

    row_number() is deliberate. rank() would give two rows the same
    number on a tie and you would still have duplicates.
    """

    window = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())

    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def explode_pipe_delimited(df: DataFrame, source_col: str,
                           target_col: str) -> DataFrame:
    """
    Split a pipe-delimited column into one row per value.

    "Adventure|Animation|Children" on movie 1 becomes three rows:
        movie 1, Adventure
        movie 1, Animation
        movie 1, Children

    This is what makes genre queries possible. Without it you would
    be writing LIKE '%Comedy%', which cannot use an index and gives
    wrong answers on edge cases.
    """
    return df.withColumn(
        target_col,
        F.explode(F.split(F.col(source_col), r"\|")),
    )


def aggregate_to_string(df: DataFrame, group_cols: list, value_col: str,
                        target_col: str, separator: str = ", ") -> DataFrame:
    """
    Collapse many rows into one, joining a column's values into a string.

    Needed for tags. A user may tag one movie fifteen times. If we
    joined the raw tags table onto ratings, that one rating row would
    become fifteen rows and every average would be wrong.

    Aggregating first gives one row per (user, movie), so the join is safe.
    """
    return (
        df.groupBy(*group_cols)
        .agg(F.concat_ws(separator, F.collect_list(F.col(value_col))).alias(target_col))
    )