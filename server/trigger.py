"""
ALTERNATIV (allerdings läufts gut!)

Je eine Datei pro ETL ablegen und
für ETLs einfach den Pfad zu der .py Datei
übergeben.

Dann exec(.py) und laufen lassen.

PLUS eine LOCK_TIME, in welcher dieser ETL nicht nochmal angestoßen werden darf
"""

from threading import Thread
from typing import Union
from dataclasses import dataclass

from flask import Flask, request

from etl.base import IETL


@dataclass(frozen=True)
class HttpETLTrigger:
    name: str
    etls: list[IETL]

    def run(self) -> None:
        """"""
        for etl in self.etls:
            etl.run()


class TriggerServer:
    """Runs a Flask http service to trigger ETL-Processes"""

    def __init__(
            self,
            *,
            name: str,
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
        self._name = name
        self._trigger = []
        self._app = Flask(__name__)
        self._running_threads: list[Thread] = []

    def run(self) -> None:
        """"""
        self._register_index_route()
        self._register_health_check_route()
        self._register_trigger_route()

        self._app.run(**self._flask_run_kwargs)

    def register(
            self,
            trigger: Union[list[HttpETLTrigger], HttpETLTrigger]
    ) -> "TriggerServer":
        self._validate_trigger(trigger=trigger)
        self._trigger += self._get_trigger_as_list(trigger)
        return self

    def _register_index_route(self) -> None:
        @self._app.route("/", methods=["GET"])
        def index() -> None:
            host = self._flask_run_kwargs["host"]
            port = self._flask_run_kwargs["port"]

            hostname = f"{host}:{port}"

            base_html_trigger_rows = """
                <tr>
                    <td>${NAME}</td>
                    <td>
                        <a href='${LINK}' target='_blank'>
                            ${LINK_DISPLAY}
                        </a>
                    </td>
                </tr>
            """

            html_trigger_rows = ""
            trigger: HttpETLTrigger
            for trigger in self._trigger:
                link = f"/api/etl/trigger?name={trigger.name}&batchsize=10000&async=0"  # noqa
                html_trigger_rows += base_html_trigger_rows \
                    .replace("${NAME}", trigger.name) \
                    .replace("${LINK}", link) \
                    .replace("${LINK_DISPLAY}", f"{hostname}{link}")

            html_table = f"""
                <table>
                    <thead>
                        <tr>
                            <th>Name</th>
                            <th>Route</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>Startpage</td>
                            <td><a href='/'>{hostname}/</a></td>
                        </tr>
                        <tr>
                            <td>Health-Check</td>
                            <td>
                                <a href='/api/health-check' target='_blank'>
                                    {hostname}/api/health-check
                                </a>
                            </td>
                        </tr>
                        {html_trigger_rows}
                    </tbody>
                </ul>
            """

            return f"""
                <h1>HttpETLTrigger-Server {self._name} :{port}</h1>
                {html_table}
            """

    def _register_health_check_route(self) -> None:
        @self._app.route("/api/health-check", methods=["GET"])
        def health_check() -> None:
            print("HELLO WORD!")
            return {
                "health-check": "ok",
                "triggers": [str(trigger) for trigger in self._trigger]
            }, 200

    def _register_trigger_route(self) -> None:
        @self._app.route("/api/etl/trigger", methods=["GET"])
        def trigger() -> None:
            # /api/etl/trigger?name=datev-dbo-client&batchsize=10000&async=0
            print("01. TRIGGERED", request.args)
            etl_name = request.args.get("name", None, type=str)
            # batchsize = request.args.get("batchsize", 10_000, type=int)
            is_async = request.args.get("async", 0, type=int)

            # if batchsize <= 0 or batchsize > 100_000:
            #     return {
            #         "error": f"The entered batchsize '{batchsize}' is out of range (> 0 && <= 100_000)",  # noqa
            #     }, 400

            etl = self._get_trigger(etl_name)
            if etl is None:
                return {
                    "error": f"The ETL-Name {etl_name} is not registered!"
                }, 400

            for etl_thread in self._running_threads:
                if etl_thread.name == etl_name:
                    if etl_thread.is_alive():
                        return {
                            "error": f"The ETL-Process '{etl_name}' is still running."  # noqa
                        }, 425  # Too Early (multiple request)

                    self._running_threads.remove(etl_thread)

            if is_async == 1:
                print("02. START ASYNC")
                etl_thread = Thread(
                    name=etl_name,
                    target=etl.run
                )

                self._running_threads.append(etl_thread)
                etl_thread.start()

                print("03. FINISHED")
                return {
                    "message": f"The ETL {etl_name} is now running."
                }, 202

            message = ""
            message_name = "message"
            result_code = 200
            try:
                print("02. START RUN")
                etl.run()
                print("03. RUN SUCCESSFULLY FINISHED")
                message = f"The ETL {etl_name} has run successfully."
            except Exception as e:
                message = f"An error has occurred with the following message:\n{str(e)}"  # noqa
                result_code = 500
                message_name = "error"
                print("03. RUN EXIT WITH ERROR")

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
