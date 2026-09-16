"""
Reusable aggregation helpers for the gold layer.

"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def mode_of(df, group_cols, value_col, output_col):
    """
    Find the most frequently occurring value in each group.

    Used for "popular tag" and "most used tag".

    Ties are broken alphabetically so the result is deterministic -
    the same input always gives the same answer.
    """
    counted = (
        df.filter(F.col(value_col).isNotNull())
        .groupBy(*group_cols, value_col)
        .agg(F.count("*").alias("_freq"))
    )

    window = Window.partitionBy(*group_cols).orderBy(
        F.col("_freq").desc(), F.col(value_col).asc()
    )

    return (
        counted.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select(*group_cols, F.col(value_col).alias(output_col))
    )


def top_row_per_group(df, group_cols, order_cols, select_col, output_col):
    """
    Pick one row per group based on a sort order.

    Used for "movie with most ratings", "worst rated movie",
    "most recently rated movie".

    order_cols is a LIST of Column expressions, so you can sort by
    several things - for example rating ascending, then year descending
    to break ties by taking the more recent film.
    """
    window = Window.partitionBy(*group_cols).orderBy(*order_cols)

    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select(*group_cols, F.col(select_col).alias(output_col))
    )


def top_n_array(df, group_cols, order_cols, value_col, output_col, n=10):
    """
    Collect the top N values in each group into an array.

    Used for "top 10 popular movies" and "top 10 highly rated movies",
    which the spec says must be Array type.

    collect_list preserves order within a partition, so ranking first
    then collecting gives the array in the right order.
    """
    window = Window.partitionBy(*group_cols).orderBy(*order_cols)

    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") <= n)
        .groupBy(*group_cols)
        .agg(F.collect_list(F.col(value_col)).alias(output_col))
    )


def add_gold_audit(df):
    """Add LoadTs and UpdateTs in UTC."""
    now = F.current_timestamp()
    return df.withColumn("LoadTs", now).withColumn("UpdateTs", now)