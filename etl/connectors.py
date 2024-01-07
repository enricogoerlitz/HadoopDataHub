""""""
import pandas as pd

from etl.base import IConnector


class CsvConnector(IConnector):
    """Connector for Reading csv files in batches"""

    def __init__(self, path: str, sep: str = ",") -> None:
        super().__init__()
        self._path = path
        self._sep = sep

    @property
    def path(self) -> str:
        return self._path

    @property
    def sep(self) -> str:
        return self._sep

    def iterbatches(self, batchsize: int) -> enumerate[int, pd.DataFrame]:
        """"""
        iter_batches = pd.read_csv(
            self._path,
            chunksize=batchsize,
            sep=self._sep
        )
        return enumerate(iter_batches)

    def batch(self, batchsize: int, skiprows: int = 0) -> pd.DataFrame:
        return pd.read_csv(
            self._path,
            sep=self._sep,
            nrows=batchsize,
            skiprows=skiprows
        )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(\n" + \
                f"\t'path={self.path}',\n" + \
                f"\t'sep={self.sep}'\n" + \
                ")"

    def __repr__(self) -> str:
        return str(self)


class PostgresConnector(IConnector):
    """"""
    pass


class SqlServerConnector(IConnector):
    """"""
    pass


class MySqlConnector(IConnector):
    """"""
    pass
