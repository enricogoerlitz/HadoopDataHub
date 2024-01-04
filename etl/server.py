""""""

from typing import Union
from dataclasses import dataclass

from flask import Flask

from etl.etl import IETL


@dataclass
class HttpETLTrigger:
    route: str
    etls: list[IETL]


class FlaskServer:
    """Runs a Flask http service to trigger ETL-Processes"""

    def __init__(
            self,
            *,
            host: str = "0.0.0.0",
            port: int,
    ) -> None:
        self._trigger = []
        self._app = Flask(__name__)

    def run(self) -> None:
        """"""

        app = 
        pass

    def register(
            self,
            trigger: Union[list[HttpETLTrigger], HttpETLTrigger]
    ) -> 'FlaskServer':
        self._validate_trigger(trigger=trigger)
        self._trigger += self._get_trigger_as_list(trigger)
        return self

    def _validate_trigger(
            self,
            trigger: Union[list[HttpETLTrigger], HttpETLTrigger]
    ) -> None:
        trigger = self._get_trigger_as_list(trigger=trigger)
        for trigger_ in trigger:
            if not isinstance(trigger_, HttpETLTrigger):
                raise TypeError(
                    "A Trigger in list triggers is not an instance of HttpETLTrigger")  # noqa

    def _get_trigger_as_list(
            self,
            trigger: Union[list[HttpETLTrigger], HttpETLTrigger]
    ) -> list[HttpETLTrigger]:
        if isinstance(trigger, list):
            return trigger
        return [trigger]
