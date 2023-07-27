-- Databricks notebook source
from pyspark.sql.functions import col, rand

df = (spark.range(1, 1000000)
      .withColumn("id", (col("id") / 1000).cast("integer"))
      .withColumn("v", rand(seed=1)))
