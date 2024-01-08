"""
NEXT TODOS:

Copy UPDATE Files to tmp, DONT RENAME!!! (search: NEW LOGIC?)
 - New Logic
 - AND Only write in tmp! -> so no update in PSA!!!

USE tez as hive execution engine!!!
    - copy hive and tez foöders from docker-containers to local
    - serve the conf files in the hive and tez folders
    - copy current hive conf to BCK_{foldername}
    - change current hive conf so that taz can be used!

VALID_FROM Meta-Managed-Hive-Table (ONLY FOR HISTORIZED TABLES!)
    - create transactional HIVE Table (meta.stdhdpetl_pk)
    - sprak.sql("SELECT * FROM meta.stdhdpetl_pk)
        .filter(f.col("pk") not in new pks)
    - Columns: hdfs_path, pk, row_valid_from

implement params:
    - valid from column
    - valid to column

handle column changes
    - handle columnname has changed or column was removed:
        > 1. get current structure
        > 2. if structure has changed (a column is )
            -> notify.email send email or Whatsapp or other notification
        > 3. manually checking the structure and change it!
            -> column renamed? -> rename column in HDFS
            -> start process again
    - handle column adding
        > 1. this should be okay with the current logic

New ETLs:
- CreateStdDimTable
- CreateStdDimHist (crossjoin)
- CreateStdFact -> get the guid of dims!


- PSATable
    - .pk
    - .add_relation(PSATable, fk_columns)
    - .create_std_dim(
        select_columns={PSATable: [columns]},
        rename={PSATable: "new_name"},
        hist_dim: bool
    )
    - .create_std_fact()


"""
import uuid
import pandas as pd

from IPython.display import display
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from etl.base.etl import AbstractEtl
from etl.connectors import IConnector
from etl.notify import INotificator
from etl.clients import HDFileSystemClient, HiveClient
from etl.base.datamodels import TableDataClass
from etl.enums import eHdfsFileType
from etl import utils


class HadoopStdETL(AbstractEtl):
    """"""

    def __init__(
            self,
            conn: IConnector,
            hdfs_client: HDFileSystemClient,
            hive_client: HiveClient,
            tmp_path: str,
            dist_path: str,
            bck_path: str,
            table: TableDataClass,
            change_columns: list[str],
            use_spark: bool,
            historize: bool,
            batchsize: int = 10_000,
            notificators: list[INotificator] = None
    ) -> None:
        """
        ...summary

        Args:
            conn (IConnector): _description_
            hdfs_client (HDFileSystemClient): _description_
            hive_client (HiveClient): _description_
            tmp_path (str): _description_
            dist_path (str): _description_
            bck_path (str): _description_
            table (TableDataClass): _description_
            use_spark (bool): use spark at large tables
        """
        self._conn = conn
        self._hdfs_client = hdfs_client
        self._hive_client = hive_client
        self._tmp_path = tmp_path
        self._dist_path = dist_path
        self._bck_path = bck_path
        self._table = table
        self._change_columns = change_columns
        self._batchsize = batchsize

        tmp_tablename = f"tmp_{self._table.table_name}"
        self._tmp_table: TableDataClass = self._table.copy(
            table_name=tmp_tablename)

        self._dist_tablepath = self._get_table_path(self._dist_path)
        self._dist_tablepath_extended = "/".join([
            self._dist_tablepath, self._table.table_name])
        self._bck_tablepath = self._get_table_path(self._bck_path)
        self._tmp_tablepath = self._get_table_path(self._tmp_path)
        self._historized = historize
        self._notoficators = notificators

        if self._historized and not isinstance(self._table.pk, list):
            raise TypeError(
                "The Table primary keys (pk) should be a list of strings.")

        if self._historized and not isinstance(self._change_columns, list):
            raise TypeError(
                "The Table ist historized, so you need to enter change_columns as list")  # noqa

        if (self._notoficators is not None and
           not isinstance(self._notoficators, list)):
            raise TypeError(
                "The given notificators are no list!"
            )

    def run(self) -> None:
        """"""
        is_psa_existing = self._hdfs_client.exists(path=self._dist_tablepath_extended)  # noqa

        df_structure = self._get_df_structure()

        # 1. Save data to tmp
        self._stage_fullload_to_tmp()

        # 2. Create external table for tmp
        self._hive_client.create_external_table(
            df=df_structure,
            table=self._tmp_table,
            filetype=eHdfsFileType.PARQUET,
            location=self._tmp_tablepath
        )

        # 3. update changed rows and insert deleted rows && move psa to bck psa
        if is_psa_existing and self._historized:
            # 3.1 Update changed rows and insert deleted rows
            print("UPDATE", self._dist_tablepath_extended)
            self._stage_handle_updates(df_structure=df_structure)

        if is_psa_existing:
            # 3.2 Rename current psa to bck_psa
            print("MOVE PSA TO BCK")
            self._hdfs_client.client.makedirs(self._bck_tablepath)
            self._hdfs_client.client.rename(
                hdfs_src_path=self._dist_tablepath,
                hdfs_dst_path=self._bck_tablepath
            )

        # 4. Rename tmp to psa
        print("MOVE TMP TO PSA")
        self._hdfs_client.client.makedirs(self._dist_tablepath)
        self._hdfs_client.client.rename(
            hdfs_src_path=self._tmp_tablepath,
            hdfs_dst_path=self._dist_tablepath,
        )

        # 5. Delete bck_psa
        if self._hdfs_client.exists(path=self._bck_tablepath):
            print("DELETE BCK")
            self._hdfs_client.client.delete(
                hdfs_path=self._bck_tablepath,
                recursive=True
            )

        # 6. Create external table for psa
        self._hive_client.create_external_table(
            df=df_structure,
            table=self._table,
            filetype=eHdfsFileType.PARQUET,
            location=self._dist_tablepath_extended
        )

    @staticmethod
    def create(name: str) -> None:
        raise NotImplementedError()

    def status(self) -> str:
        raise NotImplementedError()

    def logs(self) -> list[utils.Log]:
        raise NotImplementedError()

    def _stage_fullload_to_tmp(self) -> None:
        """"""
        print("START LOAD TO TMP")
        dt_valid_to = datetime(2100, 12, 31)

        self._hdfs_client.client.delete(self._tmp_tablepath, recursive=True)

        batch: pd.DataFrame
        for i, batch in self._conn.iterbatches(batchsize=self._batchsize):
            # 1.0 variables
            filename = f"BATCH_{i}.parquet"
            filepath_tmp = "/".join([self._tmp_tablepath, filename])
            filepath_dist = "/".join([self._dist_tablepath_extended, filename])

            # 1.1. add edw-columns
            batch = self._sfltt_transform_batch(
                batch=batch,
                dt_valid_to=dt_valid_to,
                filepath_tmp=filepath_tmp,
                filepath_dist=filepath_dist,
            )

            # 1.2 Save batch as BATCHX.parquet to tmp
            data_content = batch.to_parquet(index=False, compression="snappy")
            self._hdfs_client.write(
                data=data_content,
                hdfs_path=filepath_tmp,
                overwrite=True
            )

    def _stage_handle_updates(self, df_structure: pd.DataFrame) -> None:
        """"""
        try:
            # Create SparkSession
            spark_name = f"SPARKSESSION_FOR_{self._table.get_tablename().upper()}"  # noqa
            spark = self._hive_client.create_spark_session(spark_name)

            df_updated_rows, df_deleted_rows = self._shu_gen_updated_dfs(
                spark=spark
            )

            print("UPDATED DF:")
            display(df_updated_rows)
            print("DELETED DF:")
            display(df_deleted_rows)

            # Transform the Data

            all_files = self._hdfs_client.client.list(self._dist_tablepath_extended)  # noqa
            update_files = sorted([file for file in all_files if "UPDATE" in file])  # noqa
            update_files_reverse = sorted(update_files, reverse=True)

            write_kwargs, df_last_update_file = self._shu_get_write_kwargs(
                update_files_reverse)

            df_transformed = self._shu_transfrom_df(
                df_structure=df_structure,
                df_last_update_file=df_last_update_file,
                df_updated_rows=df_updated_rows,
                df_deleted_rows=df_deleted_rows,
                write_path=write_kwargs["hdfs_path"]
            )

            print("TRANSFORMED DF:")
            display(df_transformed)

            # TODO: DO NOT WRITE IN PSA !! ONLY WRITE TO TMP!!!
            if df_transformed is not None:
                data_content_parquet = utils.df_to_parquet_content(df_transformed)  # noqa

                self._hdfs_client.write(
                    data=data_content_parquet,
                    **write_kwargs
                )

            # TODO: Read File and write it to tmp (NO RENAME!)
            for update_filename in update_files:
                psa_filepath = "/".join(
                    [self._dist_tablepath_extended, update_filename])
                tmp_filepath = "/".join(
                    [self._tmp_tablepath, update_filename])

                # NEW LOGIC?
                # IF OKAY => self._hdfs_client.copy_file(...)
                with self._hdfs_client.client.read(psa_filepath) as reader:
                    byte_data = reader.read()
                    self._hdfs_client.write(
                        data=byte_data,
                        hdfs_path=tmp_filepath,

                    )

                # self._hdfs_client.client.rename(
                #     hdfs_src_path=psa_filepath,
                #     hdfs_dst_path=tmp_filepath
                # )

            spark.stop()
            spark = None
        except Exception as e:
            raise e
        finally:
            if spark:
                spark.stop()

    def _sfltt_transform_batch(
            self,
            batch: pd.DataFrame,
            dt_valid_to: pd.Timestamp,
            filepath_tmp: str,
            filepath_dist: str
    ) -> pd.DataFrame:
        batch["guid"] = [str(uuid.uuid4()) for _ in range(0, batch.shape[0])]
        batch["pk"] = batch[self._table.pk] \
            .apply(lambda row: '_||_'.join(map(str, row)), axis=1)

        batch["row_valid_to"] = dt_valid_to
        batch["row_valid_to"] = batch["row_valid_to"].dt.strftime("%Y-%m-%d %H:%M:%S")  # noqa

        batch["row_is_current"] = 1
        batch["row_is_deleted"] = 0
        batch["row_filepath"] = filepath_dist
        batch["row_filepath_tmp"] = filepath_tmp
        batch["is_batch_file"] = 1

        return batch

    def _shu_gen_updated_dfs(
            self,
            spark: SparkSession,

    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        tmp_query = f"SELECT * FROM {self._tmp_table.get_tablename_hive()}"
        psa_query = f"SELECT * FROM {self._table.get_tablename_hive()}" \
                    + " WHERE is_batch_file = 1"
        # Connect to PSA and TMP
        df_spark_psa = spark.sql(psa_query)
        df_spark_tmp = spark.sql(tmp_query)

        # Generate Change-Keys (Column differences)
        df_spark_psa = df_spark_psa.withColumn(
            "COLDIFF_KEY",
            f.concat_ws("_", *self._change_columns)
        )
        df_spark_tmp = df_spark_tmp.withColumn(
            "COLDIFF_KEY_TMP",
            f.concat_ws("_", *self._change_columns)
        )

        # Rename TMP-Columns for JOIN
        df_spark_tmp = df_spark_tmp.withColumnRenamed("guid", "guid_tmp")

        # Select just needed columns from TMP
        df_spark_tmp = df_spark_tmp.select(*[
            "pk",
            "guid_tmp",
            # "row_new_filepath",
            # "row_new_tmp_filepath",
            "COLDIFF_KEY_TMP"
        ])

        # JOIN TMP and PSA Data to identify changes
        df_spark_joined = df_spark_psa.join(
            df_spark_tmp, on="pk", how="left")

        # Filter df to get updated and deleted rows
        df_spark_updated_rows = df_spark_joined.filter(
            f.col("COLDIFF_KEY") != f.col("COLDIFF_KEY_TMP"))
        df_spark_deleted_rows = df_spark_joined.filter(
            f.col("COLDIFF_KEY_TMP").isNull())

        # Cellect the Data into pandas df
        df_updated_rows = pd.DataFrame(
            df_spark_updated_rows.collect(),
            columns=df_spark_updated_rows.columns
        )
        df_deleted_rows = pd.DataFrame(
            df_spark_deleted_rows.collect(),
            columns=df_spark_deleted_rows.columns
        )

        return df_updated_rows, df_deleted_rows

    def _shu_get_write_kwargs(
            self,
            update_files_sorted_reverse: list[str]
    ) -> tuple[dict, pd.DataFrame]:
        """_summary_

        Args:
            update_files_sorted (list[str]): _description_

        Returns:
            dict: _description_
        """
        # TODO: DO NOT WRITE IN PSA !! ONLY WRITE TO TMP!!!

        # No Update-Files existing
        if len(update_files_sorted_reverse) == 0:
            return {
                "hdfs_path": f"{self._tmp_tablepath}/UPDATE_0.parquet",
                "overwrite": True
            }, None
        # Identify last update file and the last number
        # (files are stored inc. like UPDATE_0..X)
        last_update_file: str = update_files_sorted_reverse[0]
        last_number = int(last_update_file.rsplit(".", 1)[0].split("_")[1])

        last_update_filepath = "/".join(
            [self._dist_tablepath_extended, last_update_file])

        hdfs_path = f"{self._dist_tablepath_extended}/{last_update_file}"

        # Read in the last Update-File for comparison and
        # maybe for appending
        df_last_update_file = self._hdfs_client.read_parquet(
            last_update_filepath)

        # append only, if the last file has lower then X rows
        # else create a new file with one higher number
        if df_last_update_file.shape[0] > 9_500:
            new_update_filename = f"UPDATE_{last_number + 1}.parquet"
            hdfs_path = f"{self._tmp_tablepath}/{new_update_filename}"

        return {
            "hdfs_path": hdfs_path,
            "overwrite": True
        }, df_last_update_file

    def _shu_transfrom_df(
            self,
            df_structure: pd.DataFrame,
            df_last_update_file: pd.DataFrame,
            df_updated_rows: pd.DataFrame,
            df_deleted_rows: pd.DataFrame,
            write_path: str
    ) -> pd.DataFrame:
        filename = write_path.rsplit("/", 1)[1]
        row_filepath = "/".join(
            [self._dist_tablepath_extended, filename]
        )

        df_deleted_rows.loc[:, "row_is_deleted"] = 1

        df_transformed = pd.concat([
            df_last_update_file,
            df_deleted_rows,
            df_updated_rows
        ], axis=0)

        if df_transformed.shape[0] == 0:
            return None

        df_transformed.loc[:, "row_is_current"] = 0
        df_transformed.loc[:, "row_valid_to"] = utils.current_dt_date()  # noqa
        df_transformed["row_valid_to"] = pd.to_datetime(
            df_transformed["row_valid_to"]) \
            .dt.strftime("%Y-%m-%d %H:%M:%S")
        df_transformed.loc[:, "row_filepath"] = row_filepath
        df_transformed.loc[:, "row_filepath_tmp"] = write_path
        df_transformed.loc[:, "is_batch_file"] = 0

        return df_transformed[df_structure.columns]

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

        ordered_columns = ["guid", "pk"] \
            + list(df_structure.columns) \
            + ["row_is_current", "row_valid_to", "row_is_deleted",
               "row_filepath", "row_filepath_tmp", "is_batch_file"]

        df_structure["guid"] = ""
        df_structure["pk"] = ""
        df_structure["row_is_current"] = 1
        df_structure["row_is_deleted"] = 1
        df_structure["row_valid_to"] = datetime.now()
        df_structure["row_filepath"] = ""
        df_structure["row_filepath_tmp"] = ""
        df_structure["is_batch_file"] = 1

        return df_structure[ordered_columns]


class HadoopMessageETL(AbstractEtl):
    """Message Queue ETL"""
    pass
