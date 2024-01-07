""""""
from etl.base.etl import IETL


class SchedulerTime:
    """"""

    def __init__(self, time: str) -> None:
        self._validate_time(time=time)
        self._time = time

    def _validate_time(self, time: str) -> None:
        """"""
        raise ValueError(f"Time bla {time}")


class ETLScheduler:
    """"""

    def __init__(
            self,
            name: str,
            etls: tuple[IETL, list[SchedulerTime]]
    ) -> None:
        pass

    def register(self, etls: tuple[IETL, list[SchedulerTime]]) -> None:
        pass

    def run(self) -> None:
        """"""
        # try except and async!
        pass
