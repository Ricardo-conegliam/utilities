# Databricks notebook source
from delta import DeltaTable
from pyspark.sql.functions import col,lit
from datetime import date, timedelta

# COMMAND ----------

spark.sql("CREATE TABLE main.schema01.events ( event_time TIMESTAMP COMMENT 'x', value BIGINT, customer STRING, id BIGINT, event STRING) USING delta TBLPROPERTIES ( 'delta.checkpoint.writeStatsAsJson' = 'false', 'delta.checkpoint.writeStatsAsStruct' = 'true', 'delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '2')")

# COMMAND ----------

spark.sql("CREATE TABLE main.schema01.events_large ( event_time TIMESTAMP COMMENT 'x', value BIGINT, customer STRING, id BIGINT, event STRING) USING delta TBLPROPERTIES ( 'delta.checkpoint.writeStatsAsJson' = 'false', 'delta.checkpoint.writeStatsAsStruct' = 'true', 'delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '2')")

# COMMAND ----------

start_date = date(2022, 1, 1)
end_date = date(2023, 7, 1)
delta = timedelta(days=1)


spark.sql("truncate table main.schema01.events_large")
while start_date <= end_date:

    data_string = start_date.strftime("%Y-%m-%d")

    print(data_string)
    start_date += delta

    for customer in range(100,150):
    
        spark.sql(f"insert into main.schema01.events_large select '{data_string}' event_time, '{customer}' customer, * except(event_time,customer) from main.schema01.events;")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- insert into main.schema01.events_large select * from main.schema01.events_large;
# MAGIC -- insert into main.schema01.events_large select * from main.schema01.events_large;
# MAGIC -- insert into main.schema01.events_large select * from main.schema01.events_large;
# MAGIC -- insert into main.schema01.events_large select * from main.schema01.events_large;
# MAGIC -- insert into main.schema01.events_large select * from main.schema01.events_large;
# MAGIC -- insert into main.schema01.events_large select * from main.schema01.events_large;
# MAGIC -- insert into main.schema01.events_large select * from main.schema01.events_large;
# MAGIC -- insert into main.schema01.events_large select * from main.schema01.events_large;
# MAGIC -- insert into main.schema01.events_large select * from main.schema01.events_large;
# MAGIC -- insert into main.schema01.events_large select * from main.schema01.events_large;
# MAGIC
# MAGIC drop table if exists main.schema01.events_zorder;
# MAGIC create table main.schema01.events_zorder deep clone main.schema01.events_large;
# MAGIC optimize main.schema01.events_zorder zorder by (customer);

# COMMAND ----------


