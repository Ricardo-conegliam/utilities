# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import current_timestamp, to_date

sTimestamp=current_timestamp()
# jsonString=f'{"key":999999,"description":"TESTE","yearmonth":"202208","lookupkey":"1","time_stamp":"{sTimestamp}"}'

data2 = [("999999","Teste","202208","1","2022-08-25 00:37:52.858"), \
          ("999997","Teste6","202209","1","2022-08-25 00:37:52.999")]


schema = StructType([ \
    StructField("key",StringType(),True), \
    StructField("description",StringType(),True), \
    StructField("yearmonth",StringType(),True), \
    StructField("lookupkey", StringType(), True), \
    StructField("time_stamp", StringType(), True)\
  ])

df = spark.createDataFrame(data=data2,schema=schema)


DupEvents = ( 
       df
       .selectExpr("CAST(key AS STRING) AS key", "to_json(struct(*)) AS value")
       .write
       .format("kafka")
       .option("kafka.bootstrap.servers", bootstrapServers)
       .option("kafka.security.protocol", "SASL_SSL")
       .option("kafka.sasl.jaas.config","kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(apiKey, apiSecret))
       .option("kafka.ssl.endpoint.identification.algorithm", "https")
       .option("kafka.sasl.mechanism", "PLAIN")
       .option("topic", eventtopicName)
       .option("checkpointLocation", checkpoint)
       .save()
)
