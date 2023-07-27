# Databricks notebook source
# MAGIC %md ### Data generator for Kafka Events
# MAGIC
# MAGIC Also see analysis notebook.
# MAGIC
# MAGIC We'll generate the dataframes, then write to kafka.

# COMMAND ----------

# MAGIC %md ### Initial Setup ###

# COMMAND ----------

from datetime import timedelta, datetime

from pyspark.sql.functions import date_format, expr, struct, lit, create_map, col, array, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,  TimestampType

from pyspark.sql.functions import (
    expr,
    current_timestamp,
    from_json,
)



# COMMAND ----------

# dbutils.widgets.removeAll()
checkpoint = "dbfs:/users/dlttest/checkpoint/"
scope_secret = "rconegliam_kafka_eh"
bootstrapServers = "fe-shared-ssa-latam-eventhubs.servicebus.windows.net:9093"
clusterName = "fe-shared-ssa-latam-eventhubs"
minCod = 100000
maxCod = 999999
batchSize = 1000

table = "DLTschema1.table1"
topic = "topic1"

schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("description", StringType(), True)
    ]
)


apiUsername = "$ConnectionString"
apiPassword = dbutils.secrets.get(scope=scope_secret, key="apiPassword")

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.rm("dbfs:/users/dlttest/",True)

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE dltschema1.table1;
# MAGIC CREATE TABLE DLTschema1.table1 (id INTEGER,description STRING) LOCATION "dbfs:/users/dlttest/table1"

# COMMAND ----------

# MAGIC %sql desc extended DLTschema1.table1

# COMMAND ----------

# DBTITLE 1,Using regular Structured Stream to ingest data to table "table1"
spark.readStream\
.format("kafka")\
.option("kafka.bootstrap.servers", bootstrapServers)\
.option("kafka.security.protocol", "SASL_SSL")\
.option(
                "kafka.sasl.jaas.config",
                "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(
                    apiUsername, apiPassword
                ),
            )\
.option("kafka.ssl.endpoint.identification.algorithm", "https")\
.option("kafka.sasl.mechanism", "PLAIN")\
.option("subscribe", topic)\
.load() \
.select(
                 from_json(col("value").cast("string"), schema).alias("json")
             )\
.selectExpr("json.*")\
.writeStream\
.format("delta")\
.option("checkpointLocation", "dbfs:/users/dlttest/checkpoint/") \
.outputMode("append") \
.table(table)

# COMMAND ----------

# DBTITLE 1,Generating data for event topic and saving to kafka topic
data = [(1,"Description 1"), (2,"Description 2"),(3,"Description 3"),(4,"Description 4"),(5,"Description 5"),(6,"Description 6"),(7,"Description 7"),(8,"Description 8"),(9,"Description 9"),(10,"Description 10")]


df = spark.createDataFrame(data,schema)


df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
.coalesce(1) \
.write \
.format("kafka") \
.option("kafka.bootstrap.servers", bootstrapServers) \
.option("kafka.security.protocol", "SASL_SSL") \
.option("kafka.sasl.jaas.config","kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(apiUsername, apiPassword)) \
.option("kafka.ssl.endpoint.identification.algorithm", "https") \
.option("kafka.sasl.mechanism", "PLAIN") \
.option("topic", topic) \
.option("checkpointLocation", checkpoint+topic) \
.save()

# COMMAND ----------

# MAGIC %sql select * from DLTschema1.table1 order by 1

# COMMAND ----------

# DBTITLE 1,Backing up table 
# MAGIC %sql
# MAGIC CREATE TABLE DLTschema1.table1_bkp DEEP CLONE DLTschema1.table1 LOCATION "dbfs:/users/dlttest/table1_bkp" ;

# COMMAND ----------

# DBTITLE 1,Stopping previews streaming
for stream in spark.streams.active:
  stream.stop()
  stream.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ##To migrate to DLT 
# MAGIC
# MAGIC 1. Stop your Structured stream (even it is streaming)
# MAGIC 2. Get the offsets numbers for all partitions on the checkpoint location for your table (table1)
# MAGIC 3. Add this offsets to startingOffsets on DLT configuration
# MAGIC     .option("startingOffsets","""{"topic1":{"0":80}}""")
# MAGIC 4. Create a NEW DLT pipeline (configure to use same DATABASE as the external table).  
# MAGIC     If DLT already exists, clean DLT checkpoints for the table you are adding (ex. dbutils.fs.rm("dbfs:/pipelines/44574746-5ba8-4b73-b998-348f54c64c78/checkpoints/table1",True))
# MAGIC 5. Check if you still have exacly once

# COMMAND ----------

# DBTITLE 1,Checking last offsets (get from higher number you find on checkpoing/offsets)
dbutils.fs.head("dbfs:/users/dlttest/checkpoint/offsets/1")

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.rm("dbfs:/pipelines/44574746-5ba8-4b73-b998-348f54c64c78/checkpoints",True)

# COMMAND ----------

# DBTITLE 1,Ingesting more data on topic to be read from DLT
data = [(11,"Description 11"), (12,"Description 12"),(13,"Description 13"),(14,"Description 14"),(15,"Description 15"),(16,"Description 16"),(17,"Description 17"),(18,"Description 18"),(19,"Description 19"),(20,"Description 20")]


df = spark.createDataFrame(data,schema)


df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
.write \
.format("kafka") \
.option("kafka.bootstrap.servers", bootstrapServers) \
.option("kafka.security.protocol", "SASL_SSL") \
.option("kafka.sasl.jaas.config","kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(apiUsername, apiPassword)) \
.option("kafka.ssl.endpoint.identification.algorithm", "https") \
.option("kafka.sasl.mechanism", "PLAIN") \
.option("topic", topic) \
.option("checkpointLocation", checkpoint+topic) \
.save()

# COMMAND ----------

# DBTITLE 1,Verifing the data (it must apper more 10 lines, with no repetition, inserted by DLT)
# MAGIC %sql select * from DLTschema1.table1 order by 1

# COMMAND ----------

# DBTITLE 1,Checking the location, it must be the same as before)
# MAGIC %sql
# MAGIC desc extended dltschema1.table1

# COMMAND ----------


