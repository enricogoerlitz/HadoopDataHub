""""""
from typing import Union
from pyspark.sql import SparkSession, \
                        DataFrame as SparkDataFrame, \
                        functions as f


DEFAULT_BATCH_ID_COLUMN_NAME = "batch_id_row"


def min_by_column(df: SparkDataFrame, column: str) -> Union[int, float]:
    """"""
    return df.agg(f.min(col=column)).collect()[0][0]


def max_by_column(df: SparkDataFrame, column: str) -> Union[int, float]:
    """"""
    return df.agg(f.max(col=column)).collect()[0][0]


def add_batch_id_column(
        df: SparkDataFrame,
        column_name: str = DEFAULT_BATCH_ID_COLUMN_NAME
) -> SparkDataFrame:
    """"""
    return df.withColumn(column_name, f.monotonically_increasing_id())


def get_next_batch_id(
        df: SparkDataFrame,
        current_id: int,
        batch_size: int,
        row_id_column: str = DEFAULT_BATCH_ID_COLUMN_NAME,
) -> int:
    """"""
    return df.filter(f.col(row_id_column) > current_id + batch_size) \
             .agg(f.min(col=row_id_column)) \
             .collect()[0][0]


def get_batch(
        spark: SparkSession,
        df: SparkDataFrame,
        next_batch_id: int,
        batch_size: int,
        row_id_column: str = DEFAULT_BATCH_ID_COLUMN_NAME,
) -> SparkDataFrame:
    """"""
    return spark.createDataFrame(
            df
            .filter((f.col(row_id_column) >= next_batch_id))
            .limit(batch_size).collect()
    )
