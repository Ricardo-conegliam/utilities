# Databricks notebook source
# MAGIC %sql
# MAGIC -- create table if not exists take.streaming_log 
# MAGIC -- ( timestamp string, id string, runId string, name string, event string);
# MAGIC
# MAGIC -- create table if not exists main.schema01.logListener_progress
# MAGIC -- ( event_id string, event_name string, payload string );
# MAGIC
# MAGIC create table if not exists main.schema01.logListener (
# MAGIC     queryId string,
# MAGIC     name string,
# MAGIC     startTime timestamp,
# MAGIC     inputRows string,
# MAGIC     processedRows string
# MAGIC )

# COMMAND ----------

deltaTableName = "main.schema01.logListener"


# COMMAND ----------

from pyspark.sql.streaming import StreamingQueryListener

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    LongType,
    BooleanType,
    TimestampType,
)

from pyspark.sql.streaming import StreamingQueryListener
from delta.tables import *

#Open Delta Table
deltaTable = DeltaTable.forName(spark, deltaTableName)

class DeltaStreamingQueryListener(StreamingQueryListener):
    def onQueryStarted(self, queryStarted):
        # print("Query started:". queryStarted.id)
        pass
        
    def onQueryProgress(self, queryProgress):

        # # Get the streaming metrics from the progress
        # query_id = queryProgress['id']
        # batch_id = queryProgress['batchId']
        # num_input_rows = queryProgress['numInputRows']
        # input_rows_per_second = queryProgress['inputRowsPerSecond']
        # # Extract more metrics fields as needed

        # # Create a DataFrame with the metrics data
        # metrics_df = spark.createDataFrame([(query_id, batch_id, num_input_rows, input_rows_per_second)], schema)

        # # Append the metrics to the Delta table
        # # delta_table = DeltaTable.forPath(spark, self.delta_table_path)
        # deltaTable.alias("existing_data").merge(metrics_df.alias("new_data"), "existing_data.queryId = new_data.queryId") \
        #     .whenNotMatched().insertAll() \
        #     .execute()

        
        name = queryProgress.progress.name
        sources = queryProgress.progress.sources
        num_input_rows = queryProgress.progress.numInputRows
        batch_tags = [f"streaming_query_name:{name}"]
        # statsd.gauge("databricks.stream.num_input_rows",num_input_rows,batch_tags)

        for s in sources:
            descriptio  n = s.description
            metrics = s.metrics
            start_offset = s.startOffset
            end_offset = s.endOffset
            latest_offset = s.latestOffset
            tags = [f"streaming_query_name:{name}",f"streaming_source:{description}"]
            for k, v in metrics.items():
                # statsd.gauge(f"databricks.stream.{k}",v,tags)
                print(f"databricks.stream.{k} - {v} - streaming_query_name:{name}",f"streaming_source:{description}")

    def onQueryTerminated(self, queryTerminated):
        pass




# COMMAND ----------

my_listener = DeltaStreamingQueryListener()
spark.streams.addListener(my_listener)

# COMMAND ----------

df = spark.readStream.table("main.schema01.events")

dfw = df.writeStream \
    .trigger(availableNow=True)  \
    .option("checkpointLocation","dbfs:/tmp/check") \
    .option("mergeSchema", "true") \
    .table("main.schema01.events2")

dfw.awaitTermination()

spark.streams.removeListener(my_listener)

# COMMAND ----------

# from pyspark.sql.functions import count, col, lit, max
# monitor_stream = stream.observe("metric",count(lit(1)).alias("cnt"),  # number of processed rows
#                                          max(col("offset")).alias("last_offset"))  # number of malformed rows
# my_query = monitor_stream.writeStream.format(
#     "console").queryName("My observer 2").start()

# COMMAND ----------

# class DLTProgressListener(StreamingQueryListener):
#   def onQueryStarted(self, event):
#     pass
#   def onQueryProgress(self, event):
#     name = event.progress.name
#     sources = event.progress.sources
#     num_input_rows = event.progress.numInputRows
#     batch_tags = [f"streaming_query_name:{name}"]
#     statsd.gauge("databricks.stream.num_input_rows",num_input_rows,batch_tags)

#     for s in sources:
#       description = s.description
#       metrics = s.metrics
#       start_offset = s.startOffset
#       end_offset = s.endOffset
#       latest_offset = s.latestOffset
#       tags = [f"streaming_query_name:{name}",f"streaming_source:{description}"]
#       for k, v in metrics.items():
#         statsd.gauge(f"databricks.stream.{k}",v,tags)
      
#   def onQueryTerminated(self, event):
#     pass

# dlt_listener = DLTProgressListener()
# spark.streams.addListener(dlt_listener)

# COMMAND ----------


