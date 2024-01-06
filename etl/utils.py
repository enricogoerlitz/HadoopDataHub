"""
"""
import pandas as pd

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from abc import ABC, abstractmethod


@dataclass(frozen=True)
class Log:
    time: datetime
    level: Literal
    message: str


# TODO: MOVE THIS!
class IMetaFile(ABC):
    """"""

    @abstractmethod
    def read(self, path: str) -> None:
        pass

    @abstractmethod
    def write(self, path: str, data: dict) -> None:
        pass


class HadoopEtlMetafile(IMetaFile):
    """"""

    def __init__(self, filename: str = "__META") -> None:
        pass

    def write(self, path: str, data: dict) -> None:
        # .json file, but without .json, only __META
        pass


def current_dt_date() -> pd.Timestamp:
    return pd.to_datetime(
        datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0))


def df_to_parquet_content(df: pd.DataFrame) -> Any:
    return df.to_parquet(index=False, compression="snappy")
