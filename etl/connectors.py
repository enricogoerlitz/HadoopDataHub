"""
"""
import uuid
import pandas as pd

from typing import Any

from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyhive import hive

from etl.enums import eHdfsFileType
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
            host: HostDataClass,
            thrift_port: int = None
    ) -> None:
        self._host = host
        self._thrift = f"thrift://{host.host}:{thrift_port}" \
                       if thrift_port is not None \
                       else None

    @property
    def host(self) -> HostDataClass:
        return self._host

    @property
    def thrift(self) -> str:
        return self._thrift

    def get_databases(self) -> list:
        """"""
        query = "SHOW DATABASES"
        return self.execute_with_list_result(query)

    def get_tables(self, database: str = "default") -> list:
        """"""
        query = f"SHOW TABLES IN {database}"
        return self.execute_with_list_result(query)

    def describe_table(self, table: TableDataClass) -> pd.DataFrame:
        """"""
        query = f"DESCRIBE {table.database}.{table.table_name}"
        return self.execute_with_df_result(query)

    def read_table(
            self,
            table: TableDataClass,
            columns: list[str] = None,
            limit: int = None
            ) -> pd.DataFrame:
        """Reads a table and returns a pandas dataframe"""
        query = f"SELECT * FROM {table.database}.{table.table_name}"
        if columns is not None:
            columns_query_str = ",".join(columns)
            query = query.replace("*", columns_query_str)
        if limit is not None:
            query += f" LIMIT {limit}"

        return self.execute_with_df_result(query)

    def execute_hive_query(self, query: str) -> tuple[list[tuple], list]:
        """"""
        connection = self._get_connection()
        cursor = connection.cursor()

        try:
            cursor.execute(query)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            return result, column_names
        except Exception as e:
            raise e
        finally:
            cursor.close()
            connection.close()

    def execute_with_df_result(self, query: str) -> pd.DataFrame:
        """"""
        result = self.execute_hive_query(query)
        return self.parse_queryresult_to_df(result)

    def execute_with_list_result(self, query: str) -> list:
        """"""
        result = self.execute_hive_query(query)[0]
        return [item[0] for item in result]

    def parse_queryresult_to_df(
            self,
            queryresult: tuple[list[tuple], list]
            ) -> pd.DataFrame:
        """"""
        result, columns = queryresult
        return pd.DataFrame(result, columns=columns)

    def create_spark_session(self, session_name: str = None) -> SparkSession:
        """"""
        if self.thrift is None:
            err = "thrift port is None. Please add the thrift port of the Hive server"  # noqa
            raise ValueError(err)
        if session_name is None:
            session_name = str(uuid.uuid4())
        return SparkSession.builder \
            .appName(session_name) \
            .config("hive.metastore.uris", self.thrift) \
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

    def _get_connection(self) -> hive.Connection:
        """"""
        return hive.Connection(host=self.host.host, port=self.host.port)

    def delete_external_table(self, table_name: TableDataClass) -> None:
        """"""
        pass

    def _get_drop_table_statement(self, table_name: TableDataClass) -> str:
        """"""
        return f"DROP TABLE IF EXISTS {table_name}"

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
