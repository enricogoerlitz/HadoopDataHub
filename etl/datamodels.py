""""""
from dataclasses import dataclass

from etl.base.datamodels import AbstractDataClass, copydataclass


@copydataclass
@dataclass(frozen=True)
class HostDataClass(AbstractDataClass):
    """"""
    host: str
    port: int

    @property
    def hostname(self) -> str:
        """"""
        return f"{self.host}:{self.port}"


@copydataclass
@dataclass(frozen=True)
class TableDataClass(AbstractDataClass):
    """"""
    table_name: str
    database: str
    schema: str = None
    pk: list[str] = None

    def get_tablename(self) -> str:
        """"""
        if self.schema is None:
            return f"{self.database}.{self.table_name}"
        return f"{self.database}.{self.schema}.{self.table_name}"

    def get_tablename_hive(self) -> str:
        """"""
        if self.schema is None:
            return f"{self.database}.{self.table_name}"
        return f"{self.database}.{self.schema}_{self.table_name}"
