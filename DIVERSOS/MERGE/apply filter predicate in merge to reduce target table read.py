# Databricks notebook source
# MAGIC %md 
# MAGIC ### How to restrict date ranges when merging

# COMMAND ----------

# example - restricting date ranges in table merge from streaming dataframe
# first, let's create some data and write it to a table

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType

schema = StructType(
    [StructField("record",StringType(),True),
     StructField("record_date",StringType(),True),
     StructField("value",IntegerType(),True)
    ]
)

streaming_data = [("record 1","2022-08-20",3000),
                ("record 2","2022-09-20",34000)
               ]

table_data = [("record 1","2022-08-20",1000),
                ("record 2","2022-08-20",2000)
             ]

df_streaming = spark.createDataFrame(data=streaming_data,schema=schema).select(["record", to_date("record_date").alias("record_date"), "value"])
df_table = spark.createDataFrame(data=table_data,schema=schema).select(["record", to_date("record_date").alias("record_date"), "value"])

df_table.write.format("delta").mode("overwrite").saveAsTable("conegliam_uc.events.teste_merge_with_date_ranges")

# COMMAND ----------

# MAGIC %sql select * from conegliam_uc.events..teste_merge_with_date_ranges

# COMMAND ----------

# now let's collect the min and max date from the streaming dataframe, and use it in our merge statement
from pyspark.sql.functions import min, max

def updateRecordsWithDateRangePredicate(batch_df, batch_id):
    # collects the date ranges in the incoming data frame
    row = batch_df.select(min("record_date"), max("record_date")).collect()[0]
    min_date, max_date = [row[i] for i in (0,1)]
    
    # now let's merge with the delta table using the range as a filter predicate
    deltaTable = DeltaTable.forName(spark, "conegliam_uc.events.teste_merge_with_date_ranges")

    (deltaTable.alias("t")
         .merge(batch_df.alias("i"),
                "t.record = i.record AND t.record_date BETWEEN '{}' AND '{}'".format(min_date, max_date))
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    print ("Query:", "t.record = i.record AND t.record_date BETWEEN '{}' AND '{}'".format(min_date, max_date))
    
updateRecordsWithDateRangePredicate(df_streaming, 0)

# COMMAND ----------

# MAGIC %sql select * from conegliam_uc.events.teste_merge_with_date_ranges
