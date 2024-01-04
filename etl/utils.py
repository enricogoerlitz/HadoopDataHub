"""
"""
import pandas as pd

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal


@dataclass(frozen=True)
class Log:
    time: datetime
    level: Literal
    message: str


def current_dt_date() -> pd.Timestamp:
    return pd.to_datetime(
        datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0))


def df_to_parquet_content(df: pd.DataFrame) -> Any:
    return df.to_parquet(index=False, compression="snappy")
