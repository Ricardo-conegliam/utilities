// Databricks notebook source
// MAGIC %md ### local hbase and kafka instructions
// MAGIC
// MAGIC For development we're using local, non-kerberized hbase and kafka installs. This notebook explains the configuration needed, followed by tests.
// MAGIC
// MAGIC The cluster *bci-hadoop-migration-local-hbase-non-kerberos* has already been configured and should be ready to use.
// MAGIC
// MAGIC For HBASE:
// MAGIC - use the following init script in the cluster
// MAGIC - install ch.cern.hbase.connectors.spark:hbase-spark:1.0.1_spark-3.0.1_4 package
// MAGIC - after starting, if you need to access the hbase shell, use the terminal
// MAGIC - hbase is installed in /local_disk0/hbase-2.1.6, hbase shell should be working from ./bin
// MAGIC
// MAGIC For Kafka:
// MAGIC - the second init script will install kafka and zookeeper

// COMMAND ----------

// MAGIC %md ### HBASE

// COMMAND ----------

dbutils.fs.put(s"/CONF/install_hbase.sh","""#!/bin/sh

# hdinsights uses hbase 2.1.6...
wget -qO- https://archive.apache.org/dist/hbase/2.1.6/hbase-2.1.6-bin.tar.gz | tar xz -C /local_disk0

# configuration file
cat <<EOF>/local_disk0/hbase-2.1.6/conf/hbase-local-non-kerberos-site.xml
<configuration>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>localhost</value>
  </property>
  <property>
    <name>zookeeper.znode.parent</name>
    <value>/hbase</value>
  </property>
</configuration>
EOF

# starts hbase
export JAVA_HOME=/usr/lib/jvm/zulu8-ca-amd64/jre/
cd /local_disk0/hbase-2.1.6 && ./bin/start-hbase.sh
""", true)

// COMMAND ----------

// MAGIC %md ### before testing below, connect to the terminal and create the table with hbase shell
// MAGIC
// MAGIC ```
// MAGIC cd /local_disk0/hbase-2.1.6
// MAGIC ./bin/hbase shell
// MAGIC
// MAGIC create 'Person', 'Name', 'Address'
// MAGIC put 'Person', '1', 'Name:First', 'Raymond'
// MAGIC put 'Person', '1', 'Name:Last', 'Tang'
// MAGIC put 'Person', '1', 'Address:Country', 'Australia'
// MAGIC put 'Person', '1', 'Address:State', 'VIC'
// MAGIC
// MAGIC put 'Person', '2', 'Name:First', 'Dnomyar'
// MAGIC put 'Person', '2', 'Name:Last', 'Gnat'
// MAGIC put 'Person', '2', 'Address:Country', 'USA'
// MAGIC put 'Person', '2', 'Address:State', 'CA'
// MAGIC ```

// COMMAND ----------

import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path

val conf = HBaseConfiguration.create()
conf.addResource(new Path("/local_disk0/hbase/conf/hbase-local-non-kerberos-site.xml"))
new HBaseContext(spark.sparkContext, conf)

// COMMAND ----------

val mapping = """rowKey STRING :key,
  firstName STRING Name:First,
  lastName STRING Name:Last,
  country STRING Address:Country,
  state STRING Address:State"""

// COMMAND ----------

val hbaseDF = (spark.read.format("org.apache.hadoop.hbase.spark")
 .option("hbase.columns.mapping", mapping
 )
 .option("hbase.table", "Person")
 ).load()

// COMMAND ----------

display(hbaseDF)

// COMMAND ----------

// MAGIC %md ### Kafka and Zookeeper

// COMMAND ----------

dbutils.fs.put(s"/CONF/install_kafka.sh","""#!/bin/sh

#installs and starts zookeeperd
sudo apt-get install -y zookeeperd && systemctl start zookeeper

# get kafka distribution
wget -qO- https://dlcdn.apache.org/kafka/3.2.0/kafka_2.12-3.2.0.tgz | tar xz -C /local_disk0

# starts kafka
(cd /local_disk0/kafka_2.12-3.2.0 && ./bin/kafka-server-start.sh -daemon config/server.properties)
""", true)

// COMMAND ----------

// MAGIC %sh
// MAGIC # create kafka topic
// MAGIC cd /local_disk0/kafka_2.12-3.2.0 &&
// MAGIC bin/kafka-topics.sh --create --topic tbjnl01 --bootstrap-server localhost:9092
