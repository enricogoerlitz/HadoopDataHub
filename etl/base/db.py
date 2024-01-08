""""""
from abc import ABC, abstractmethod

from etl.base.datamodels import TableDataClass


class IQuery(ABC):
    """"""

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def select(
        self,
        table: TableDataClass,
        columns: list[str] = None,
        tbl_as: str = "tbl"
    ) -> 'IQuery':
        """"""
        pass

    @abstractmethod
    def where(self, condition: str) -> 'IQuery':
        """"""
        pass

    @abstractmethod
    def join(
        self,
        table: TableDataClass,
        on: str,
        tbl_as: str
    ) -> 'IQuery':
        """"""
        pass

    @abstractmethod
    def leftjoin(
        self,
        table: TableDataClass,
        on: str,
        tbl_as: str
    ) -> 'IQuery':
        """"""
        pass

    @abstractmethod
    def rightjoin(
        self,
        table: TableDataClass,
        on: str,
        tbl_as: str
    ) -> 'IQuery':
        """"""
        pass

    @abstractmethod
    def fulljoin(
        self,
        table: TableDataClass,
        on: str,
        tbl_as: str
    ) -> 'IQuery':
        """"""
        pass

    @abstractmethod
    def crossjoin(
        self,
        table: TableDataClass,
        tbl_as: str
    ) -> 'IQuery':
        """"""
        pass

    @abstractmethod
    def build(self) -> str:
        """"""
        pass


class AbstractQuery(IQuery):
    """"""

    def __init__(self) -> None:
        super().__init__()
