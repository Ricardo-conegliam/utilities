# Databricks notebook source
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


cSchema = StructType(
    [StructField("word", StringType()), StructField("stimestamp", StringType())]
)

data = [("teste1", "2019-06-24 12:01:19.000"), ("teste2", "2019-06-24 12:01:19.000")]

# COMMAND ----------

data

# COMMAND ----------

cSchema

# COMMAND ----------

words = spark.createDataFrame(data,cSchema)
# ...  # streaming DataFrame of schema { timestamp: Timestamp, word: String }


words = words.withColumn("timestamp",to_timestamp(col("stimestamp")))



# COMMAND ----------

words.display()

# COMMAND ----------

windowedCounts = words \
   .withWatermark("timestamp", "10 minutes") \
   .groupBy(

    window("timestamp", "10 minutes", "5 minutes"),
    words.word
).count()


# COMMAND ----------

windowedCounts.display()


# COMMAND ----------


