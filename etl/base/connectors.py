""""""
import pandas as pd

from abc import ABC, abstractmethod


class IConnector(ABC):
    """Interface for all Connectors"""

    @abstractmethod
    def iterbatches(self, batchsize: int) -> enumerate[int, pd.DataFrame]:
        """"""
        pass

    @abstractmethod
    def batch(self, batchsize: int, skiprows: int) -> pd.DataFrame:
        """"""
        pass
