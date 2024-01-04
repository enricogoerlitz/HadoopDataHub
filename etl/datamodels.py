""""""
from dataclasses import dataclass, fields


class AbstractDataClass:
    def copy(self, **kwargs) -> 'TableDataClass':
        valid_fields = {f.name for f in fields(self)}
        invalid_keys = set(kwargs.keys()) - valid_fields
        if invalid_keys:
            raise ValueError(f"Invalid keys: {invalid_keys}")

        updated_fields = {
            f.name: kwargs.get(f.name, getattr(self, f.name))
            for f in fields(self)
        }

        copied_instance = TableDataClass(**updated_fields)
        return copied_instance


@dataclass(frozen=True)
class HostDataClass(AbstractDataClass):
    """"""
    host: str
    port: int

    @property
    def hostname(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True)
class TableDataClass(AbstractDataClass):
    """"""
    table_name: str
    database: str
    schema: str = None
    pk: list[str] = None

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
