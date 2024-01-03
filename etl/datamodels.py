""""""
from dataclasses import dataclass


@dataclass(frozen=True)
class HostDataClass:
    """"""
    host: str
    port: int

    @property
    def hostname(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True)
class TableDataClass:
    """"""
    table_name: str
    database: str
    schema: str = None
    pk: list[str] = None
