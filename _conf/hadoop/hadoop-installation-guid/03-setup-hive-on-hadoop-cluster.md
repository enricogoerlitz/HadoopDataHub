# Setting up a HIVE Cluster on top of a Hadoop cluster

0. Create Hadoop-Cluster

1. ANDERS ALS HIER
- https://hub.docker.com/r/apache/hive
- https://hub.docker.com/r/apache/hive/tags
- https://hive.apache.org/developement/quickstart/ ==> how to enter JDBC beeline
- install HIVE VERSION mit Docker
- dann kopiere tez und hive dirs from /opt/tez und /opt/hive
- passe die Konfigurationen an
- starte hadoop und die beiden HIVE services (nicht im Container!)
- ==> vielleicht kann man dann auch mehr mit HiveQL (joins etc) arbeiten, um dinge herauszufinden...

0. 1. USE DOCKER => https://hub.docker.com/r/apache/hive
- docker pull apache/hive:4.0.0-alpha-1
- export HIVE_VERSION_DOCKER=4.0.0-alpha-1
- docker run -d -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 --name hive4test2 apache/hive:${HIVE_VERSION_DOCKER}

**Mount the mysql driver**
docker run -d -p 9083:9083 --env SERVICE_NAME=metastore --env DB_DRIVER=mysql \
 --env SERVICE_OPTS="-Djavax.jdo.option.ConnectionDriverName=com.mysql.jdbc.Driver -Djavax.jdo.option.ConnectionURL=jdbc:mysql://192.168.64.102:3306/metastore?createDatabaseIfNotExist=true -Djavax.jdo.option.ConnectionUserName=hadoop -Djavax.jdo.option.ConnectionPassword=hadoop_pw" \
--mount type=bind,source=/home/enricogoerlitz/apache-hive-2.3.9-bin/lib/mysql-connector-j-8.1.0.jar,target=/opt/hive/lib/mysql-connector-j-8.1.0.jar \
--name metastore-standalone apache/hive:${HIVE_VERSION_DOCKER}

--mount source=warehouse,target=/opt/hive/data/warehouse \


1. Start Hadoop (HADOOP_PATH/sbin/start-all-sh)
2. Donwload
- https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz

- sudo wget https://downloads.apache.org/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz
- sudo tar -xvzf apache-hive-2.3.9-bin.tar.gz
3. Install RDBS (e.a. MySQL)
    - sudo apt-get install mysql-server
    - sudo mysql -u root -p 
        - Enter password (e.a. root)
4. Download MySQL Connector/j
    - wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.1.0/mysql-connector-j-8.1.0.jar
    - sudo mv ./mysql-connector-j-8.1.0.jar HIVE_PATH/lib/mysql-connector-j-8.1.0.jar
5. Configure remote access to MySQL
    5.1 Remove bind-address:
        - sudo nano /etc/mysql/mysql.conf.d/mysqld.cnf
        - comment out: 'bind-address: 127.0.0.1' and 'mysqlx-bind-address: 127.0.0.1'
    5.2 Create Service-User
        - sudo mysql -u root -p
        - mysql> CREATE USER 'hadoop'@'%' IDENTIFIED BY 'hadoop_pw';
        - mysql> GRANT ALL PRIVILEGES ON *.* TO 'hadoop'@'%' WITH GRANT OPTION;
    5.3 Restart MySQL Server, so the changes of Bind-Address will effected
        - sudo systemctl restart mysql
        - sudo service mysql restart
    5.4 Test Connection with DBeaver to SQL-Server
6. Configure HIVE
        - sudo nano $HIVE_HOME/bin/hive-config.sh 
            ==> export HADOOP_HOME=/home/....{HADOOP-PATH}
        - sudo nano ./.bashrc
            ==> export HIVE_HOME=/path/to/hive
            ==> export PATH=$PATH:HIVE_HOME/bin:...
        - sudo touch HIVE_PATH/conf/hive-site.xml
        - set hive-site.xml:

<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
	<property>
		<name>javax.jdo.option.ConnectionURL</name>
		<value>jdbc:mysql://192.168.64.102:3306/metastore?createDatabaseIfNotExist=true</value> # hier richte IP eintrage (NameNode, bzw wo mysql server ist)
	</property>
	<property>
		<name>javax.jdo.option.ConnectionDriverName</name>
		<value>com.mysql.jdbc.Driver</value>
		<!-- <value>com.mysql.jc.jdbc.Driver</value>  ==> THIS IS RECOMMENDED! SO FIND THE .jar-->
	</property>
	<property>
		<name>javax.jdo.option.ConnectionUserName</name>
		<value>hadoop</value>
	</property>
	<property>
		<name>javax.jdo.option.ConnectionPassword</name>
		<value>hadoop_pw</value>
	</property>
	<property>
		<name>hive.metastore.schema.verification</name>
		<value>false</value>
	</property>
    <property>
        <name>hive.server2.enable.doAs</name>
        <value>false</value> 
    </property>
</configuration>

execute:
apache-hive-3.1.3/bin/schematool -dbType mysql -initSchema


7. Start HiveServer2 und metastore
    - hive --service hiveserver2
        -> für Remote DB (DBeaver -> 192.168.64.102:10000)
    - hive --service metastore -p 9088
        - metastore für SPARK -> .config("hive.metastore.uris", "thrift://192.168.64.102:10000")
    - hadoop-3.3.6/sbin/mr-jobhistory-daemon.sh stop historyserver

NOTE::: SELECT SUM(column) FROM TABLE DOES NOT WORK WITH MR ON HIVE2 !

ERRORS WITH HIVE 2:
- Loading class `com.mysql.jdbc.Driver'. This is deprecated. The new driver class is `com.mysql.cj.jdbc.Driver'. The driver is automatically registered via the SPI and manual loading of the driver class is generally unnecessary.

- Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.

=> So use Spark as execution-engine! <br>
=> Download a different mysql-driver!


-- DOCKER

- https://hub.docker.com/r/apache/hive

sudo docker run -d -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 \
--env SERVICE_OPTS="-Dhive.metastore.uris=thrift://192.168.64.102:9083" \
--env IS_RESUME="true" \
--name hiveserver2-standalone apache/hive:${HIVE_VERSION}


docker run -d -p 9083:9083 --env SERVICE_NAME=metastore --env DB_DRIVER=mysql \
 --env SERVICE_OPTS="-Djavax.jdo.option.ConnectionDriverName=com.mysql.jdbc.Driver -Djavax.jdo.option.ConnectionURL=jdbc:mysql://192.168.64.102:3306/metastore?createDatabaseIfNotExist=true -Djavax.jdo.option.ConnectionUserName=hadoop -Djavax.jdo.option.ConnectionPassword=hadoop_pw" \
--mount source=warehouse,target=/opt/hive/data/warehouse \
--name metastore-standalone apache/hive:${HIVE_VERSION}

