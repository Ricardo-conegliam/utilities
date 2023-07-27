# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Creating table for use as DLT source (with changefeed on)
# MAGIC
# MAGIC #### Source table
# MAGIC create table source1 (id integer,description string) ;
# MAGIC desc detail source1;
# MAGIC
# MAGIC
# MAGIC #### DLT Table with changefeed
# MAGIC desc detail table1;
# MAGIC
# MAGIC {"delta.checkpoint.writeStatsAsJson":"false","delta.checkpoint.writeStatsAsStruct":"true","delta.enableChangeDataFeed":"true"}
# MAGIC
# MAGIC ##### Location derived by schema.   dbfs:/user/hive/warehouse/hotmart.db/table1
# MAGIC
# MAGIC
# MAGIC #### queries for testing
# MAGIC
# MAGIC create table source1 (id integer,description string) ;
# MAGIC desc detail source1;
# MAGIC desc detail table1;
# MAGIC
# MAGIC insert into source1 values (1,"teste1");
# MAGIC
# MAGIC select * from source1;
# MAGIC select * from table1;
# MAGIC select * from feed1;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

@dlt.table( name = "table1",
    table_properties = {
            "quality": "silver" ,
            "delta.enableChangeDataFeed" : "true",
            "enableChangeDataFeed" : "true"
            }
    )
def createTable():
    df = spark.readStream.table("hotmart.source1")\
    .withColumn("silver_timestamp",current_timestamp())
    return df

# COMMAND ----------

@dlt.table( name = "feed1",
    table_properties = {
            "quality": "silver" 
            }
    )
def createTable():
    df = spark.readStream.format("delta")\
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("live.table1")
    return df
