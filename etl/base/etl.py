""""""
from etl import utils

from abc import ABC, abstractmethod


class IETL(ABC):
    """"""

    @abstractmethod
    def run(self, *args, **kwargs) -> None:
        """"""
        pass

    @abstractmethod
    def create(self, name: str) -> None:
        """"""
        pass

    @abstractmethod
    def status(self) -> str:
        """"""
        pass

    @abstractmethod
    def logs(self) -> list[utils.Log]:
        """"""
        pass


class AbstractEtl(IETL):
    pass
