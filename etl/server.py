""""""

from threading import Thread
from typing import Union
from dataclasses import dataclass

from flask import Flask, request

from etl.etl import IETL


@dataclass
class HttpETLTrigger:
    name: str
    etls: list[IETL]


class FlaskServer:
    """Runs a Flask http service to trigger ETL-Processes"""

    def __init__(
            self,
            *,
            host: str = "0.0.0.0",
            port: int,
            threaded: bool = True,
            debug: bool = False
    ) -> None:
        self._flask_run_kwargs = {
            "host": host,
            "port": port,
            "threaded": threaded,
            "debug": debug
        }
        self._trigger = []
        self._app = Flask(__name__)
        self._running_threads: list[Thread] = []

    def run(self) -> None:
        """"""
        self._register_health_check_route()
        self._register_trigger_route()

        self._app.run(**self._flask_run_kwargs)

    def register(
            self,
            trigger: Union[list[HttpETLTrigger], HttpETLTrigger]
    ) -> "FlaskServer":
        self._validate_trigger(trigger=trigger)
        self._trigger += self._get_trigger_as_list(trigger)
        return self

    def _register_health_check_route(self) -> None:
        @self._app.route("/api/health-check", methods=["GET"])
        def health_check() -> None:
            return {
                "health-check": True,
                "triggers": self._trigger
            }

    def _register_trigger_route(self) -> None:
        @self._app.route("/api/etl/trigger", methods=["GET"])
        def trigger() -> None:
            # /api/etl/trigger?name=datev-dbo-client&batchsize=10000&async=0
            etl_name = request.args.get("name", None, type=str)
            batchsize = request.args.get("batchsize", 10_000, type=int)
            is_async = request.args.get("async", 0, type=int)

            if batchsize <= 0 or batchsize > 100_000:
                return {
                    "error": f"The entered batchsize '{batchsize}' is out of range (> 0 && <= 100_000)"  # noqa
                }, 400

            etl = self._get_trigger(etl_name)
            if etl is None:
                return {
                    "error": f"The ETL-Name {etl_name} is not registered!"
                }, 400

            for etl_thread in self._running_threads:
                if etl_thread.name == etl_name:
                    if etl_thread.is_alive():
                        return {
                            "error": f"The ETL-Process '{etl_name}'"
                        }, 425  # Too Early (multiple request)

                    self._running_threads.pop(etl_thread)

            if is_async == 1:
                kwargs = {"batchsize": batchsize}
                etl_thread = Thread(
                    name=etl_name,
                    target=etl.run,
                    kwargs=kwargs
                )

                self._running_threads.append(etl_thread)
                etl_thread.start()

                return {
                    "message": f"The ETL {etl_name} is now running."
                }, 202

            message = ""
            message_name = "message"
            result_code = 200
            try:
                etl.run(batchsize=batchsize)
                message = f"The ETL {etl_name} is now running."
            except Exception as e:
                message = f"An error has occurred with the following message:\n{str(e)}"  # noqa
                result_code = 500
                message_name = "error"

            return {
                message_name: message
            }, result_code

    def _validate_trigger(
            self,
            trigger: Union[list[HttpETLTrigger], HttpETLTrigger]
    ) -> None:
        trigger = self._get_trigger_as_list(trigger=trigger)
        for trigger_ in trigger:
            if not isinstance(trigger_, HttpETLTrigger):
                raise TypeError(
                    "A Trigger in list triggers is not an instance of HttpETLTrigger")  # noqa

    def _get_trigger(self, triggername: str) -> IETL:
        """"""
        trigger: HttpETLTrigger
        for trigger in self._trigger:
            if trigger.name == triggername:
                return trigger

        return None

    def _get_trigger_as_list(
            self,
            trigger: Union[list[HttpETLTrigger], HttpETLTrigger]
    ) -> list[HttpETLTrigger]:
        if isinstance(trigger, list):
            return trigger
        return [trigger]
