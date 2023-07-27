# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 08 : Structured Streaming with Apache Kafka or Azure EventHub
# MAGIC In the practical use for structured streaming (see "Exercise 07 : Structured Streaming (Basic)"), you can use the following input as streaming data source :
# MAGIC - **Azure Event Hub** (1st-party supported Azure streaming platform)
# MAGIC - **Apache Kafka** (streaming platform integrated with open source ecosystem, which is also used in Azure HDInsight)
# MAGIC - **Azure Cosmos DB Change Feed** (set ```ChangeFeedCheckpointLocation``` in configuration, which is better for lambda architecture)
# MAGIC
# MAGIC You can also sink to various destinations for your business needs.    
# MAGIC Especially you can push back analyzed (predicted, windowed, etc) data into Kafka topic or Event Hub again. (See the following diagrams.)
# MAGIC
# MAGIC ![Structured Streaming](https://tsmatz.github.io/images/github/databricks/20191114_Structured_Streaming.jpg)
# MAGIC
# MAGIC In this exercise, we use Azure EventHub as streaming source and output into the memory (show results) for your understanding.
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparation (Set up Event Hub and library installation)
# MAGIC Before starting,
# MAGIC 1. Create Event Hub Namespace resource in Azure Portal
# MAGIC 2. Create new Event Hub in the previous namespace
# MAGIC 3. Create SAS policy and copy connection string on generated Event Hub entity
# MAGIC 4. Install Event Hub library as follows
# MAGIC     - On workspace, right-click "Shared". From the context menu, select "Create" > "Library"
# MAGIC     - Install "com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.1" on "Maven Coordinate" source
# MAGIC     - Attach installed library to your cluster

# COMMAND ----------

# MAGIC %md
# MAGIC Read stream from Azure Event Hub as streaming dataframe using ```readStream()```.    
# MAGIC You must set your namespace, entity, policy name, and key for Azure Event Hub in the following command.

# COMMAND ----------

# Read Event Hub's stream
conf = {}
conf["eventhubs.connectionString"] = "Endpoint=sb://conegliamnamespace02.servicebus.windows.net/;SharedAccessKeyName=totalsas;SharedAccessKey=uH+UeeXBnmUdGOVnbZd+FqT+LTv0/b24hJ+WC6JIHIs=;EntityPath=topico1"

read_df = (
  spark
    .readStream
    .format("eventhubs")
    .options(**conf)
    .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC We start streaming computation by defining the sink as streaming query named "read_hub".    
# MAGIC ```start()``` function kicks off the streaming and continue to run as background jobs ...

# COMMAND ----------

# Write streams into memory
from pyspark.sql.types import *
import  pyspark.sql.functions as F

read_schema = StructType([
  StructField("event_name", StringType(), True),
  StructField("event_time", StringType(), True)])
decoded_df = read_df.select(F.from_json(F.col("body").cast("string"), read_schema).alias("payload"))

query1 = (
  decoded_df
    .writeStream
    .format("memory")
    .queryName("read_hub")
    .start()
)

# COMMAND ----------

# MAGIC %sql select payload.event_name, payload.event_time from read_hub

# COMMAND ----------

# MAGIC %md
# MAGIC Add events into EventHub and see the previous query's result again. (See how streaming is displayed in the previous background job.)

# COMMAND ----------

from pyspark.sql import Row

write_schema = StructType([StructField("body", StringType())])
write_row = [Row(body="{\"event_name\":\"Open\",\"event_time\":\"1540601000\"}")]
write_df = spark.createDataFrame(write_row, write_schema)

(write_df
  .write
  .format("eventhubs")
  .options(**conf)
  .save())

# COMMAND ----------

# MAGIC %md
# MAGIC After completed, cancel (stop) previous jobs.

# COMMAND ----------

for s in spark.streams.active:
    s.stop()
