"""
"""
import uuid
import pandas as pd

from io import BytesIO
from typing import Any

from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyhive import hive

from etl.enums import eHdfsFileType
from etl.datamodels import HostDataClass, TableDataClass


class HDFileSystemClient:
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

    def read_parquet(self, hdfs_path: str) -> pd.DataFrame:
        """"""
        df: pd.DataFrame
        with self.client.read(hdfs_path) as reader:
            byte_data = BytesIO(reader.read())
            df = pd.DataFrame(pd.read_parquet(byte_data))

        return df

    def exists(self, path: str) -> bool:
        """"""
        try:
            _ = self.client.status(path)
            return True
        except Exception:
            return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(\n" + \
                f"\thost={self.host.hostname},\n" + \
                f"\tuser={self._hdfs_username},\n" + \
                f"\turl={self.url},\n" + \
                f"\tclient={self.client}\n" + \
                ")"

    def __repr__(self) -> str:
        return str(self)


class HiveClient:
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

    def get_connection(self) -> hive.Connection:
        """"""
        return hive.Connection(host=self.host.host, port=self.host.port)

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
            limit: int = None,
            skiprows: int = None
            ) -> pd.DataFrame:
        """Reads a table and returns a pandas dataframe"""
        query = f"SELECT * FROM {table.database}.{table.table_name}"

        if columns is not None:
            columns_query_str = ",".join(columns)
            query = query.replace("*", columns_query_str)

        if limit is not None:
            query += f" LIMIT {limit}"

        if skiprows is not None:
            query += f" OFFSET {skiprows}"

        return self.execute_with_df_result(query)

    def read_table_iterbatches(
            self,
            table: TableDataClass,
            columns: list[str] = None,
            batchsize: int = 10_000
    ) -> None:
        skiprows = batchsize
        while True:
            df_table = self.read_table(
                table=table,
                columns=columns,
                limit=batchsize,
                skiprows=skiprows
            )
            if df_table.shape[0] == 0:
                break
            yield df_table
            skiprows += batchsize

    def execute_hive_query(self, query: str) -> None:
        """"""
        connection = self.get_connection()
        cursor = connection.cursor()

        try:
            cursor.execute(query)
        except Exception as e:
            raise e
        finally:
            cursor.close()
            connection.close()

    def execute_hive_query_with_result(
            self,
            query: str
            ) -> tuple[list[tuple], list]:
        """"""
        connection = self.get_connection()
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
        result = self.execute_hive_query_with_result(query)
        return self.parse_queryresult_to_df(result)

    def execute_with_list_result(self, query: str) -> list:
        """"""
        result = self.execute_hive_query_with_result(query)[0]
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
            df: pd.DataFrame,
            table: TableDataClass,
            filetype: eHdfsFileType,
            location: str,
            csv_delimiter: str = "|"
            ) -> None:
        """"""
        create_db_stmt = self._get_create_database_stmt(db_name=table.database)
        drop_stmt = self._get_drop_externaltable_stmt(table=table)
        create_stmt = self._get_create_external_table_stmt(
            df=df,
            table=table,
            filetype=filetype,
            location=location,
            csv_delimiter=csv_delimiter,
        )

        connection = self.get_connection()
        cursor = connection.cursor()

        try:
            cursor.execute(create_db_stmt)
            cursor.execute(drop_stmt)
            cursor.execute(create_stmt)
        except Exception as e:
            raise e
        finally:
            cursor.close()
            connection.close()

    def delete_external_table(
            self,
            table: TableDataClass
    ) -> tuple[list[str], list]:
        """"""
        query = self._get_drop_externaltable_stmt(table=table)
        return self.execute_hive_query(query)

    def _get_create_database_stmt(
            self,
            db_name: str
    ) -> str:
        query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
        return query

    def _get_create_external_table_stmt(
            self,
            df: pd.DataFrame,
            table: TableDataClass,
            filetype: eHdfsFileType,
            location: str,
            csv_delimiter: str = "|"
    ) -> str:
        """
        Generates the CREATE EXTERNAL TABLE stmt for hive.
        """
        def get_hive_column_dtype(column: str, pd_dtype: str) -> str:
            match str(pd_dtype):
                case "int64":
                    return f"`{column}` BIGINT"
                case "float64":
                    return f"`{column}` DOUBLE"
                case "bool":
                    return f"`{column}` BOOLEAN"
                # case "datetime64[us]":
                #     return f"`{column}` TIMESTAMP"  # TODO: Test; Change to STRING and cast in VIEW  # noqa
                # case "datetime64[ns]":
                #     return f"`{column}` TIMESTAMP"  # TODO: Test; Change to STRING and cast in VIEW  # noqa
                # case "<M8[ns]":
                #     return f"`{column}` TIMESTAMP"  # TODO: Test; Change to STRING and cast in VIEW  # noqa
                case _:
                    return f"`{column}` STRING"
        columns = [get_hive_column_dtype(column_name, dtype)
                   for column_name, dtype in df.dtypes.items()]

        hive_create_external_stmt = (
            f"CREATE EXTERNAL TABLE IF NOT EXISTS {table.database}.{table.table_name} (\n"  # noqa
            f"    {', '.join(columns)}\n"
            ")\n"
            f"{self._get_hive_stored_as(filetype, csv_delimiter)}"
            f"LOCATION '{location}'"
        )

        return hive_create_external_stmt

    def _get_drop_externaltable_stmt(
            self,
            table: TableDataClass
    ) -> str:
        """"""
        query = f"DROP TABLE IF EXISTS {table.database}.{table.table_name}"
        return query

    def _get_hive_stored_as(
            self,
            filetype: eHdfsFileType,
            csv_delimiter: str
    ) -> str:
        """"""
        if filetype == eHdfsFileType.PARQUET:
            return "STORED AS PARQUET\n"

        return (
                "ROW FORMAT DELIMITED\n"
                f"FIELDS TERMINATED BY '{csv_delimiter}'\n"
                f"STORED AS TEXTFILE\n"
            )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(\n" + \
                f"\thost={self.host.hostname},\n" + \
                f"\thrift={self.thrift},\n" + \
                ")"

    def __repr__(self) -> str:
        return str(self)
