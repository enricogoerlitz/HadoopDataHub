""""""
from abc import ABC, abstractmethod
from typing import Any
from dataclasses import fields


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


def copydataclass(cls):
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

    setattr(cls, 'copy', copy_method)

    return cls
