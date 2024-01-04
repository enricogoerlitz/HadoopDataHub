import sys

sys.path.append("./")
sys.path.append("../")
sys.path.append("../../")

import connection as connsettings  # noqa

from etl.clients import HiveClient, HDFileSystemClient  # noqa
from etl.datamodels import HostDataClass, TableDataClass  # noqa
from etl.connectors import CsvConnector  # noqa
from etl.etl import HadoopStdETL  # noqa
from etl.server import FlaskServer, HttpETLTrigger  # noqa

hive_host = HostDataClass(host=connsettings.HIVE_HOST, port=connsettings.HIVE_PORT)  # noqa
hdfs_host = HostDataClass(host=connsettings.HDFS_HOST, port=connsettings.HDFS_PORT)  # noqa
hdfs_client = HDFileSystemClient(hdfs_host=hdfs_host, hdfs_username="enricogoerlitz")  # noqa
hive_client = HiveClient(host=hive_host, thrift_port=connsettings.HIVE_THRIFT_PORT)  # noqa

# THE PATH IS RELATIVE TO THE SELECTED TERMINAL PATH!
conn = CsvConnector(path="./test/notebooks/database/datev.dbo.client.csv", sep="|")  # noqa

client_etl = HadoopStdETL(
    conn=conn,
    hdfs_client=hdfs_client,
    hive_client=hive_client,
    tmp_path="/edw/hive/tmp/",
    bck_path="/edw/hive/bck_psa/",
    dist_path="/edw/hive/psa/",
    table=TableDataClass(
        database="datev",
        schema="dbo",
        table_name="client",
        pk=["id"]
    ),
    use_spark=True,
    change_columns=["name", "change_field"],
    historize=True,
    batchsize=3
)

trigger = [
    HttpETLTrigger(name="datev-dbo-client", etls=[client_etl])
]

server = FlaskServer(
    name="Test-Server",
    host="localhost",
    port=3003,
    threaded=True,
    debug=True
).register(trigger=trigger)

server.run()
