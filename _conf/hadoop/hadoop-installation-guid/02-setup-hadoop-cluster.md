# Hadoop Multi-Cluster Setup

## 0. Resources

- https://medium.com/@festusmorumbasi/installing-hadoop-on-ubuntu-20-04-4610b6e0391e
- https://www.youtube.com/watch?v=98UCknD8_qA&list=PLLa_h7BriLH2UYJIO9oDoP3W-b6gQeA12&ab_channel=DataEngineering
- https://www.youtube.com/watch?v=_iP2Em-5Abw&t=622s&ab_channel=DataEngineering


## 1. SSH-Keys

### 1.1 generate SSH-Keys

- ssh-keygen -t rsa
- cd .ssh/
- cat id_rsa.pub

**Keys** <br>

- MASTER: ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCqxdICe1bHegUMwvZNsC+fSw0Y3lIOrtIl+iGfWITq2vVnXSmi2/FsGlV3BA1EXkqIoldomn6CcjyGiGlsUPimfMnBZCsieHtSiRW2Hr7DRSBnyKKHXcrtS/qdHqhcY+0sqVb878S2CFxJdt3zUMuk8fD5F+vtcEOH6In5alRL8XMlRcZV/HVyaDzWj+CnhldV3A/tbs4eBlkmBH9deU7xLflm8NZLpNTg6xUXmMYVnUNct51yXdCNPktP8haac1N3WpxOmcfX2q2e8S2O7fFZLy29xOGmE8+MA07PyJbdb0Fbg6w6W+CR2b2KAjXm/oRQwKilniznWOxCByLQuvToiwEd/ToDzNraWuufSocdbapM9KuQx+GqRYbMVj9P6d2EniTNszU3fwEyj41ph+Qk+Lk2AbG/twE1Y3FkfZv75/8HuOURhxOfotsdcNGZalZyKOr/Y1yF5Gauqq34xWOQe+zWjz9YnpuVTHdHLfVQzLoyt9f4SJzb3pa7VWKRdO8= ubuntu@ip.192.168.64.102


- DATANODE-1: ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCnjf6lKeqw3qwyEZeoTSZxLumv7IuTK3/N95b8CUQHdFqStNF/LYxgTllYLbHwMh7Ihib4KZT5Ivy96d9mHdJp7QBnfVxe9Mq1fgZA+0fsAhd99jiUMpbDtPGjgGEdud3CVnr4LQclBB6pCdLRQlmKrWkAIC0K1ZTzSwi84Dz0rxKAVKm9zUAiP734usDR2kb5BFxL54PRwvLrd1XxwROisASTOgbvioE+9m4TZMqi8984cNru6bn/9pJNkHr9N/isRXH7ezWLR4iThaq0a3B9W5sa6bUxdDGTJh/io3mNjALvJa1zxC/4+I6sPDVDnxvuy41Lzea1k0iStQHtRCcnHGczavaC4avaWMbcM2DVyaKznUpRkOVOA12Ne/bUNWVbNDSxmPs7cpe/4di65io/JVPtFXvQD0QQsjTuulFDj6+pgcRJoSeOYHDQiYy+/Hc1uwC3UwiEtuz6bLET86NYfysdzo0K41C/grCfXcxaQ3DHeaE5OkZBMQUM5GW3Dwk= ubuntu@ip.192.168.64.111


- DATANODE-2: ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCOkvyV+3M7+gknLdKEk0M3KO1KUkv0IazDyI/9ESwMafjgwXvKDI3DkCGK56RB1ssmOENoE6lliS1I5XYI+HEbSC+ASojLNH/u5qDB3GAznN5vaeEKCtbo8IEfz0eGFbLSDL38VtZjmDcE9/80FbRE4RgV0VITyv9kb3XczQFFziEYt3V9gG+F8Zm/z6QNO3yeU/DMHRWxkYng0E7URYcqbNdcOTV0cglO62gH2SdadJ+fVJOZ4U9Nq3J4WX0WB5B4i1ptibK80/gf4oaL1R72nRkPK/8XQw8MuH8/gtnmgckXwoiC7euete1hFq4zl4u5jfWS081SK8bGIT8qfyV76dvuBebER3gM8qilRg4CtkeMNm6PuTOLFh1JLV7dMyQ1eoVHpLtbt/Tvhk92znMTSROcPhvKF2tB4t7WKGwJ+L5Myb8YJoVmWhCiTPbd8WZoXYKfJK5BfI21FoKL/qh9bPAWGqNls7N8EOxjyEqW6H4u+FYcnmWWt8pPSp7wsiE= ubuntu@ip.192.168.64.112


- DATANODE-3: ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCfZ47oErE0zdcMokLhBasSurNY9j+iISr9jorlmwKBMjylOoTNjVKU/xwHE2a0kDbe+wL/ec7FY85if8WZbd8557cwxHWogt9F4V3/qCgGCszg6ngKsBL1Q962R/FyDsdY6lhrY1erpbdGF6QVOn5/pboo+e9zUdIs1aVc5s/oATIKSJKOdsiglP3tP+3EPwAn3e0YTV2AYeHvYAfU+pqD2GOAM+biSC1X4b/PlolEF+UatXmT+92qzcKbalmfpxRBljYoCOtCOPfW5kdI4UXazsXWwdiJziGBVrhn3fqu8tD2QrKSG4d+H+uI2KyuZdzyq7wkDqtTjUs/iHrpZvIw4+TlbpAcecPMrJYM6GNVWkMbw8c+YdnHqYzSXB8idEybCX3Lr2HEBgTegHGfci3dCnjNSoOCbjAnEYwNiYnVZiJC4kXqKISaX6rmSq5dZcO0CoUflNz9qSOYh9VepSuCLRS1dsQKq8jcycd/G9AZakZq/p99J9G+/nWm0LCDxnc= ubuntu@ip.192.168.64.113

### 1.2 Add all keys to all nodes

- cd .ssh/
- sudo nano authorized_keys -> ssh-rsa {key1} \n ssh-rsa {key2}
- auf allen nodes muss die ssh verbindung zu den anderen nodes ohne passwort funktionieren! d.h.: ssh 192.168.XXX.XXX und man ist direkt verbunden -> anders kann Hadoop keine Verbindung herstellen!

## 2. Donwload and install Resources

### 2.1 JDK

- prep: sudo apt update
- donwload JDK 8: sudo apt install openjdk-8-jdk // sudo apt install default-jdk -- or 
	- HIVE only works with JDK 8 !!!
- set JAVA_HOME
    - java installation path: sudo update-alternatives --config java -- here you can set the auto version!@
    - sudo nano ./.bashrc
        =>  export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64/jre
            export PATH=$JAVA_HOME/bin:$PATH
        => source ./.bashrc
        => echo $JAVA_PATH

### 2.2 Hadoop

- sudo wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
- sudo tar -xvzf hadoop-3.3.6.tar.gz

### 2.3 Set ENVs in .bashrc

- sudo nano ./.bashrc:
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64/jre
export HADOOP_HOME=/home/enricogoerlitz/hadoop-3.3.6

export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH

**Beschreibung** <br>
HADOOP und andere Dienste schauen nach "JAVA_HOME", um JAVA ausführen zu können. HADOOP_HOME wird hinzugefügt, damit man auf "hadoop" und auf "hdfs" zugreifen kann (im Terminal).

## 3. Configure Hadoop-Cluster

### 3.1 env.sh files

For all these:
1. HADOOP_PATH/etc/hadoop/hadoop-env.sh
2. HADOOP_PATH/etc/hadoop/mapred-env.sh
3. HADOOP_PATH/etc/hadoop/yarn-env.sh

SET the (correct) path to jdk (8 or 11):
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64/jre

### 3.2 HADOOP_PATH/etc/hadoop/core-site.xml

	<property>
		<name>fs.default.name</name>
		<value>hdfs://localhost:50000</value> # hier hdfs://{MASTER_NODE_IP}:50000
	</property>

### 3.3 HADOOP_PATH/etc/hadoop/yarn-site.xml

	<property>
		<name>yarn.nodemanager.aux-services</name>
		<value>mapreduce_shuffle</value>
	</property>
	<property>
		<name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
		<value>org.apache.hadoop.mapred.ShuffleHandler</value>
	</property>
	<property>
		<description>The hostname of the RM.</description>
		<name>yarn.resourcemanager.hostname</name> 
		<value>localhost</value> # nutze ip des namenode, weil diese config auch in allen SLAVES kommt
	</property>
	<property>
		<description>The address of the applications manager interface in the RM.</description>
		<name>yarn.resourcemanager.address</name>

	# HIER -> die NameNode-IP angeben, nicht "{yarn.resourcemanager.hostname}" -> 192.168.64.XXX:8032 !!!
		<value>{yarn.resourcemanager.hostname}:8032</value>
	</property>

### 3.4 HADOOP_PATH/etc/hadoop/hdfs-site.xml

	<property> # use this that the datanode can be registered
		<name>dfs.namenode.datanode.registration.ip-hostname-check</name>
		<value>false</value>
	</property>

	<property>  # this is when the server is namenode
		<name>dfs.namenode.name.dir</name>
		<value>/home/{username}/hadoop3.X.X-dir/namenode-dir</value>
	</property>

	<property> # this is when the server is worker-node (or both)
		<name>dfs.datanode.data.dir</name>
		<value>/home/{username}/hadoop3.X.X-dir/datanode-dir</value>
	</property>

### 3.5 HADOOP_PATH/etc/hadoop/mapred-site.xml

	<property>
		<name>mapreduce.framework.name</name>
		<value>yarn</value>
	</property>
		<property>
		<name>mapreduce.job.tracker</name>
		<value>localhost:54311</value>
	</property>
	<property>
        	<name>yarn.app.mapreduce.am.env</name>
        	<value>HADOOP_MAPRED_HOME=/home/enricogoerlitz/hadoop-3.3.6</value>
	</property>
	<property>
    	    <name>mapreduce.map.env</name>
    	    <value>HADOOP_MAPRED_HOME=/home/enricogoerlitz/hadoop-3.3.6</value>
	</property>
	<property>
    	    <name>mapreduce.reduce.env</name>
    	    <value>HADOOP_MAPRED_HOME=/home/enricogoerlitz/hadoop-3.3.6</value>
	</property>

	<!-- new test mapred settinsg for hive performance
<property>  
    <name>yarn.app.mapreduce.am.resource.mb</name>  
    <value>1228</value>
</property>
<property> 
    <name>yarn.app.mapreduce.am.command-opts</name> 
    <value>-Xmx983m</value>
</property>
<property>
    <name>mapreduce.map.memory.mb</name>
    <value>1228</value>
</property>
<property>
    <name>mapreduce.reduce.memory.mb</name>
    <value>1228</value>
</property>
<property>
    <name>mapreduce.map.java.opts</name>
    <value>-Xmx983m</value>
</property>
<property>
    <name>mapreduce.reduce.java.opts</name>
    <value>-Xmx983m</value>
</property>
-->

### 3.6 HADOOP_PATH/etc/hadoop/worker (or slaves)

#### in NameNode

list all ip addresses, enter after each <br>
192.168.XXX.XX1 <br>
192.168.XXX.XX2 <br>
192.168.XXX.XX3 <br>

#### in WorkerNode

localhost


## 4. Formate NameNode

$HADOOP_HOME/bin/hadoop namenode -format

## 5. Start Hadoop Cluster

$HADOOP_HOME/sbin/start-all.sh

## 6 Start MapReduce Job History Server

$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver

**Check** <br>
Im NameNode sollten folgende Dienste laufen (jps)
- NameNode
- SecondaryNameNode
- ResourceManager
- JobHistoryServer
- Jps

In den DataNodes sollten folgende Dienste laufen (jps)
- NodeManager
- Jps
- DataNode

START JOBHISTORY SERVER:
hadoop-3.3.6/sbin/start-dfs.sh
hadoop-3.3.6/sbin/start-yarn.sh
or hadoop-3.3.6/sbin/start-all.sh
hadoop-3.3.6/sbin/mr-jobhistory-daemon.sh start historyserver


hadoop-3.3.6/sbin/stop-dfs.sh
hadoop-3.3.6/sbin/stop-yarn.sh
or hadoop-3.3.6/sbin/stop-all.sh
hadoop-3.3.6/sbin/mr-jobhistory-daemon.sh stop historyserver

## 6. Hadoop links

- SPARK HDFS	 -> hdfs://192.168.64.102:50000
- ACCESS HDFS: 	 -> http://192.168.64.102:9870
- localhost:9870 -> NameNode WebUI
- localhost:8088 -> RessourceManager WebUI (nur auf NameNode Server sichtbar)
- http://192.168.64.102:19888/ -> JobHistory Server
