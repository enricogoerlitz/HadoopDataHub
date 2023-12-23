""""""
import pandas as pd

from etl.datamodels import TableDataClass, HostDataClass
from etl.clients import HiveClient


class HiveTable:
    """"""

    def __init__(self, client: HiveClient, table: TableDataClass) -> None:
        self._client = client
        self._table = table

    @property
    def client(self) -> HiveClient:
        return self._client

    @property
    def table(self) -> TableDataClass:
        return self._table

    @property
    def host(self) -> HostDataClass:
        return self.client.host

    def describe(self) -> pd.DataFrame:
        """"""
        return self.client.describe_table(self.table)

    def create(self, df: pd.DataFrame, location: str) -> None:
        raise NotImplementedError()

    def read(
            self,
            columns: list[str] = None,
            limit: int = None
            ) -> pd.DataFrame:
        """"""
        return self.client.read_table(
            table=self.table,
            columns=columns,
            limit=limit
        )

    def update(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()

    def delete(self) -> None:
        raise NotImplementedError()

    def __str__(self) -> str:
        return "HiveTable(\n" \
                f"\tclient={self.client}\n" \
                f"\ttable={self.table}\n" \
               ")"

    def __repr__(self) -> str:
        return str(self)
