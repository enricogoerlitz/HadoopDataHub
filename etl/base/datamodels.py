""""""
from abc import ABC, abstractmethod
from typing import Any, Type, Union
from dataclasses import fields, dataclass


class ICopyDataClass(ABC):
    """"""

    @abstractmethod
    def copy(self, **kwargs) -> Any:
        pass


class AbstractDataClass(ICopyDataClass):
    """"""

    def __init__(self) -> None:
        super().__init__()

    def copy(self, **kwargs) -> Any:
        raise NotImplementedError()


def copydataclass(cls: Type):
    def copy_method(self, **kwargs) -> Any:
        valid_fields = {f.name for f in fields(self)}
        invalid_keys = set(kwargs.keys()) - valid_fields

        if invalid_keys:
            raise ValueError(f"Invalid keys: {invalid_keys}")

        updated_fields = {
            f.name: kwargs.get(f.name, getattr(self, f.name))
            for f in fields(self)
        }

        copied_instance = cls(**updated_fields)
        return copied_instance

    setattr(cls, "copy", copy_method)

    return cls


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
    pk: Union[str, list[str]] = None
    pk_concat_str: str = None

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

    def get_pk(self) -> str:
        """"""
        return self.pk_concat_str.join(self.pk)
