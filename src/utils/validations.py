from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def check_not_null(df: DataFrame, columns: list) -> DataFrame:
    """
    Filters out records where any of the given columns are null.
    """
    condition = None
    for column in columns:
        if condition is None:
            condition = col(column).isNotNull()
        else:
            condition = condition & col(column).isNotNull()

    return df.filter(condition)