# Databricks notebook source
import pandas as pd
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession
from pyspark.sql import Window

# COMMAND ----------

spark_session = SparkSession.builder.getOrCreate()

# lets define a demonstration DataFrame to work on
df_data = {'partition': ['a','a', 'a', 'a', 'b', 'b', 'b', 'c', 'c',],
           'col_1': [1,1,1,1,2,2,2,3,3,], 
           'aggregation': [1,2,3,4,5,6,7,8,9,],
           'ranking': [4,3,2,1,1,1,3,1,5,],
           'lagging': [9,8,7,6,5,4,3,2,1,],
           'cumulative': [1,2,4,6,1,1,1,20,30,],
          }
df_pandas = pd.DataFrame.from_dict(df_data)
# create spark dataframe
df = spark_session.createDataFrame(df_pandas)

df.show()
