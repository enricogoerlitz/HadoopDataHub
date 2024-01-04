""""""

from abc import ABC, abstractmethod


class INotificator(ABC):
    """"""

    @abstractmethod
    def notify(self, message: dict) -> None:
        """
        Send a custom message to e.g. a web service or
        save data logs into a database or any usage of your needs
        """
        pass
