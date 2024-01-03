""""""
import uuid
import pandas as pd

from abc import ABC, abstractmethod
from etl.connectors import IConnector
from etl.clients import HDFileSystemClient, HiveClient
from etl.datamodels import TableDataClass
from etl.enums import eHdfsFileType


class IETL(ABC):
    """"""

    @abstractmethod
    def start(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def extend(self, *args, **kwargs) -> None:
        pass


class HadoopStdETL(IETL):
    """"""

    def __init__(
            self,
            conn: IConnector,
            hdfs_client: HDFileSystemClient,
            hive_client: HiveClient,
            tmp_path: str,
            dist_path: str,
            bck_path: str,
            table: TableDataClass
    ) -> None:
        self._conn = conn
        self._hdfs_client = hdfs_client
        self._hive_client = hive_client
        self._tmp_path = tmp_path
        self._dist_path = dist_path
        self._bck_path = bck_path
        self._table = table
        self._tmp_table = TableDataClass(
            database=self._table.database,
            schema=self._table.schema,
            table_name=f"tmp_{self._table.table_name}"
        )

        if not isinstance(table.pk, list):
            raise TypeError(
                "The Table primary keys (pk) should be a list of strings.")

    def start(
            self,
            batchsize: int = 10_000
    ) -> None:
        """"""
        # 0. Setup variables
        hdfs_dist_path = self._get_table_path(self._dist_path)
        hdfs_dist_path_extended = "/".join([
            hdfs_dist_path, self._table.table_name])
        hdfs_bck_path = self._get_table_path(self._bck_path)
        hdfs_tmp_path = self._get_table_path(self._tmp_path)

        is_psa_existing = self._hdfs_client.exists(path=hdfs_dist_path)

        df_structure = self._get_df_structure()

        # 1. Save data to tmp
        self._stage_save_to_tmp(batchsize=batchsize)

        # 2. Create external table for tmp
        self._hive_client.create_external_table(
            df=df_structure,
            table=self._tmp_table,
            filetype=eHdfsFileType.PARQUET,
            location=hdfs_tmp_path
        )

        # 3. update changed rows and insert deleted rows && move psa to bck psa
        if is_psa_existing:
            # 3.1 Update changed rows and insert deleted rows
            print("UPDATE", hdfs_dist_path_extended)
            self._stage_modify_update()
            raise ValueError("tmp impl")
            print("DELETE")
            self._stage_modify_delete()

            # 3.2 Rename current psa to bck_psa
            print("RENAME PSA TO BCK")
            self._hdfs_client.client.makedirs(hdfs_bck_path)
            self._hdfs_client.client.rename(
                hdfs_src_path=hdfs_dist_path,
                hdfs_dst_path=hdfs_bck_path
            )

        # 4. Rename tmp to psa
        print("RENAME TMP TO PSA")
        self._hdfs_client.client.makedirs(hdfs_dist_path)
        self._hdfs_client.client.rename(
            hdfs_src_path=hdfs_tmp_path,
            hdfs_dst_path=hdfs_dist_path,
        )

        # 5. Delete bck_psa
        if self._hdfs_client.exists(path=hdfs_bck_path):
            print("DELETE BCK")
            self._hdfs_client.client.delete(
                hdfs_path=hdfs_bck_path,
                recursive=True
            )

        # 2. Create external table for psa
        self._hive_client.create_external_table(
            df=df_structure,
            table=self._table,
            filetype=eHdfsFileType.PARQUET,
            location=hdfs_dist_path_extended
        )

    def extend(self, etls: list[IETL]) -> None:
        """
        Extend this ETL with other ETL-Processes,
        which will executed after the successfull run of the Process.
        """
        # TODO: implement the loop with exception handling!
        for etl in etls:
            etl.start()

    def _stage_save_to_tmp(self, batchsize: int) -> None:
        """"""
        hdfs_tmp_path = self._get_table_path(self._tmp_path)
        dt_now = pd.Timestamp.now()
        dt_valid_to = pd.Timestamp.max

        batch: pd.DataFrame
        for i, batch in self._conn.iterbatches(batchsize=batchsize):
            # 1.0 variables
            filename = f"BATCH{i}.parquet"
            filepath = "/".join([hdfs_tmp_path, filename])

            # 1.1. add edw-columns
            batch = self._transform_batch(
                batch=batch,
                dt_now=dt_now,
                dt_valid_to=dt_valid_to,
                filepath=filepath
            )

            # 1.2 Save batch as BATCHX.parquet to tmp
            data_content = batch.to_parquet(index=False, compression="snappy")
            self._hdfs_client.write(
                data=data_content,
                hdfs_path=filepath,
                overwrite=True
            )
            print(filepath)

    def _stage_modify_update(self) -> None:
        """"""
        # pandas or spark? -> pandas!
        # hive read table with LIMIT 2 OFFSET 2;
        # hive_client => read_table_iterbatches(query, batchsize) mit yield?
        # 1. join on ID, where ROW_IS_CURRENT = 1

        # 2. Select just the old updated rows and columns

        # 3. update the columns:
        #       - ROW_IS_CURRENT = 0
        #       - ROW_VALID_TO = TODAY()

        # 4. write thise data to a batchfile (override = FALSE!)
        raise NotImplementedError()

    def _stage_modify_delete(self) -> None:
        """"""
        # hive read table with LIMIT 2 OFFSET 2;
        raise NotImplementedError()

    def _transform_batch(
            self,
            batch: pd.DataFrame,
            dt_now: pd.Timestamp,
            dt_valid_to: pd.Timestamp,
            filepath: str
    ) -> pd.DataFrame:
        # batch.apply(lambda _: str(uuid.uuid4()), axis=1)
        batch["GUID"] = [str(uuid.uuid4()) for _ in range(0, batch.shape[0])]
        batch["PK"] = batch[self._table.pk] \
            .apply(lambda row: '_||_'.join(map(str, row)), axis=1)

        batch["ROW_VALID_FROM"] = dt_now
        batch["ROW_VALID_FROM"] = batch["ROW_VALID_FROM"].dt.strftime("%Y-%m-%d %H:%M:%S")  # noqa
        batch["ROW_VALID_TO"] = dt_valid_to
        batch["ROW_VALID_TO"] = batch["ROW_VALID_TO"].dt.strftime("%Y-%m-%d %H:%M:%S")  # noqa

        batch["ROW_IS_CURRENT"] = 1
        batch["ROW_FILEPATH"] = filepath

        return batch

    def _get_table_path(self, path: str) -> str:
        """"""
        tablepath = "/".join([
            self._table.database,
            self._table.schema if self._table.schema else "default",
            self._table.table_name
        ])

        fullpath = f"{path}/{tablepath}".replace("//", "/")
        return fullpath

    def _get_df_structure(self) -> pd.DataFrame:
        """"""
        # TODO: seperate this to Connector -> IConnector get df structure
        df_structure = self._conn.batch(
            batchsize=10,
            skiprows=0
        )

        ordered_columns = ["GUID", "PK"] \
            + list(df_structure.columns) \
            + ["ROW_IS_CURRENT", "ROW_VALID_FROM",
               "ROW_VALID_TO", "ROW_FILEPATH"]

        df_structure["GUID"] = ""
        df_structure["PK"] = ""
        df_structure["ROW_IS_CURRENT"] = 1
        df_structure["ROW_VALID_FROM"] = pd.Timestamp.now()
        df_structure["ROW_VALID_TO"] = pd.Timestamp.now()
        df_structure["ROW_FILEPATH"] = ""

        return df_structure[ordered_columns]
