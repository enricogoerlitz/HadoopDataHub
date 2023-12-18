"""_summary_
"""
import uuid


from typing import Any
from hdfs import InsecureClient
from pyspark.sql import SparkSession


class Connector:
    """
    """

    def __init__(self) -> None:
        pass

    def connect():
        pass


class HadoopDistributedFileSystem:
    """
    """
    def __init__(
            self,
            *,
            hdfs_host: str,
            hdfs_port: int = 9870,
            hdfs_username: str,
    ) -> None:
        self._hdfs_host = hdfs_host
        self._hdfs_port = hdfs_port
        self._hdfs_username = hdfs_username
        self._client = InsecureClient(
            f'http://{hdfs_host}:{hdfs_port}',
            user=hdfs_username
        )

    @property
    def client(self) -> InsecureClient:
        return self._client

    def write(
            self,
            data: Any,
            hdfs_path: str,
            log: bool = False,
            **write_kwargs
    ) -> None:
        with self.client.write(hdfs_path, **write_kwargs) as hdfs_file:
            hdfs_file.write(data)

        # TODO implement logging


class Hive:
    """
    """
    def __init__(
            self,
            host: str,
            port: int = 9088,
    ) -> None:
        self._host = host
        self._port = port
        self._thrift = f"thrift://{host}:{port}"

    def create_spark_session(self, session_name: str = None) -> SparkSession:
        """"""
        if not session_name:
            session_name = str(uuid.uuid4())
        return SparkSession.builder \
            .appName(session_name) \
            .config("hive.metastore.uris", self._thrift) \
            .enableHiveSupport() \
            .getOrCreate()
