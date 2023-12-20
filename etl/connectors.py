"""
"""
import uuid
import pandas as pd

from typing import Any

from hdfs import InsecureClient
from pyspark.sql import SparkSession

from enums import eHdfsFileType
from etl.datamodels import HostDataClass, TableDataClass


class HadoopDistributedFileSystem:
    """
    """
    def __init__(
            self,
            *,
            hdfs_host: HostDataClass,
            hdfs_username: str,
    ) -> None:
        self._host = hdfs_host
        self._hdfs_username = hdfs_username
        self._hdfs_url = f"http://{hdfs_host.hostname}"
        self._client = InsecureClient(
            url=self._hdfs_url,
            user=hdfs_username
        )

    @property
    def host(self) -> HostDataClass:
        return self._host

    @property
    def url(self) -> str:
        return self._hdfs_url

    @property
    def client(self) -> InsecureClient:
        return self._client

    def write(
            self,
            data: Any,
            hdfs_path: str,
            **write_kwargs
    ) -> None:
        with self.client.write(hdfs_path, **write_kwargs) as hdfs_file:
            hdfs_file.write(data)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(\n" + \
                f"\thost={self.host.hostname},\n" + \
                f"\tuser={self._hdfs_username},\n" + \
                f"\turl={self.url},\n" + \
                f"\tclient={self.client}\n" + \
                ")"

    def __repr__(self) -> str:
        return str(self)


class Hive:
    """
    """
    def __init__(
            self,
            host: HostDataClass
    ) -> None:
        self._host = host
        self._thrift = f"thrift://{host.hostname}"

    @property
    def host(self) -> HostDataClass:
        return self._host

    @property
    def thrift(self) -> str:
        return self._thrift

    @property
    def databases(self) -> list:
        raise NotImplementedError()

    @property
    def tables(self) -> list:
        raise NotImplementedError()

    def get_columns(self, table: TableDataClass) -> list:
        raise NotImplementedError()

    def read_table(table: TableDataClass) -> pd.DataFrame:
        """Reads a table and returns a pandas dataframe"""
        raise NotImplementedError()

    def create_spark_session(self, session_name: str = None) -> SparkSession:
        """"""
        if not session_name:
            session_name = str(uuid.uuid4())
        return SparkSession.builder \
            .appName(session_name) \
            .config("hive.metastore.uris", self._thrift) \
            .enableHiveSupport() \
            .getOrCreate()

    def create_external_table(
            self,
            df: Any,
            table: TableDataClass,
            filetype: eHdfsFileType
            ) -> None:
        """"""
        pass

    def update_external_table(self, table_name: TableDataClass) -> None:
        """
        Updates an existing external table.
        Drops the existing and creates a new external table
        """
        # WORK WITH CURSOR AND AT ERROR -> cursor.rollback()
        self.delete_external_table("")
        self.create_external_table("")

    def delete_external_table(self, table_name: TableDataClass) -> None:
        """"""
        pass

    def _get_drop_table_statement(self, table_name: TableDataClass) -> str:
        """"""
        return f"DROP TABLE {table_name}"

    def _get_create_external_table_statement(
            self,
            df,
            table_name: str
            ) -> str:
        """
        Generates the CREATE EXTERNAL TABLE statement for hive.
        """
        pass

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(\n" + \
                f"\thost={self.host.hostname},\n" + \
                f"\thrift={self.thrift},\n" + \
                ")"

    def __repr__(self) -> str:
        return str(self)
