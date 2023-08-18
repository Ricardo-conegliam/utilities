# Databricks notebook source
from pyspark.sql.functions import col,lit,current_timestamp
from delta import DeltaTable

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.merge_dfp.customer2 (
# MAGIC   c_custkey BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   c_name STRING,
# MAGIC   c_address STRING,
# MAGIC   c_nationkey BIGINT,
# MAGIC   c_phone STRING,
# MAGIC   c_acctbal DECIMAL(18, 2),
# MAGIC   c_mktsegment STRING,
# MAGIC   c_comment STRING,
# MAGIC   c_timestamp TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE main.merge_dfp.customer2 SET TBLPROPERTIES ("delta.targetFileSize" = "128MB");

# COMMAND ----------

dbutils.fs.rm("/Volumes/main/default/conegliam/checkpoints/customer2",True)
spark.sql("truncate table main.merge_dfp.customer2")

# COMMAND ----------

df = spark.readStream.table("main.merge_dfp.customer").drop("c_custkey").withColumn("c_timestamp",current_timestamp())

dfwrite =   ( df.writeStream
            .option("checkpointLocation","/Volumes/main/default/conegliam/checkpoints/customer2")
            .trigger(availableNow=True)
            .queryName(f"Writing table") \
            .table("main.merge_dfp.customer2")
            )

dfwrite.awaitTermination()



# COMMAND ----------

# MAGIC %sql
# MAGIC update main.merge_dfp.customer2 set c_timestamp = date_add(SECOND , c_custkey - 1861500000 , current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(c_custkey),max(c_custkey),min(c_timestamp),max(c_timestamp),count(*) from main.merge_dfp.customer2

# COMMAND ----------

# DBTITLE 1,Creating customer3 to avoid ALWAYS GENERATED fields on update
DF1 = spark.sql('SELECT * FROM main.merge_dfp.customer2 WHERE 1=1')
DF1.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("main.merge_dfp.customer3") 
ALTER TABLE main.merge_dfp.customer3 SET TBLPROPERTIES ("delta.targetFileSize" = "128MB");

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE main.merge_dfp.customer3 SET TBLPROPERTIES ("delta.targetFileSize" = "32MB");

# COMMAND ----------

# DBTITLE 1,Optimizing by ID, simulating a natural order (now using customer3)
# MAGIC %sql
# MAGIC optimize main.merge_dfp.customer3 zorder by c_custkey;

# COMMAND ----------

# MAGIC %md
# MAGIC #UPDATES

# COMMAND ----------

# DBTITLE 1,Preparing Update Sample and Checking the update id range simulating a incremental id update
def updateCreate():
    df2 = spark.read.table("main.merge_dfp.customer3")

    # test with random ids
    dfUpdates = df2.sample(0.001).withColumn("c_acctbal",col("c_acctbal")*lit(1.5)).where("c_custkey > 1500000000")

    # test with latest ids
    #dfUpdates = df2.where("c_custkey > 1861400000").withColumn("c_acctbal",col("c_acctbal")*lit(1.5))

    dfUpdates.withColumn("c_acctbal",col("c_acctbal")*lit(1.5)).write.mode("overwrite").option("mergeSchema",True).saveAsTable("main.merge_dfp.customer_updates")

    return (spark.read.table("main.merge_dfp.customer_updates"))



# COMMAND ----------

# DBTITLE 1,Merging Function
from pyspark.sql.functions import min, max

def updateRecords(batch_df, batch_id, type):

    
    # now let's merge with the delta table using the range as a filter predicate
    deltaTable = DeltaTable.forName(spark, "main.merge_dfp.customer3")

    if type == "R":   ## means the merge will be done by range on the fields
        # collects the date ranges in the incoming data frame
        row = batch_df.select(min("c_timestamp"), max("c_timestamp")).collect()[0]
        min_date, max_date = [row[i] for i in (0,1)]

        (deltaTable.alias("t")
            .merge(batch_df.alias("u"),
                    f"t.c_custkey = u.c_custkey AND t.c_timestamp BETWEEN '{min_date}' AND '{max_date}'")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
        print ("Query:", f"t.c_custkey = i.c_custkey AND t.transaction_date BETWEEN '{min_date}' AND '{max_date}'")
    else:

        (deltaTable.alias("t")
         .merge(batch_df.alias("u"),"t.c_custkey = u.c_custkey ")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        print ("Query:", "t.c_custkey = i.c_custkey")
    

# COMMAND ----------

# MAGIC %md
# MAGIC #PHOTON

# COMMAND ----------

# DBTITLE 1,Testing update with date range
dfUpdates = updateCreate()
updateRecords(dfUpdates,0,"R")

# COMMAND ----------

# DBTITLE 1,Testing update checking if Dynamic File Prunning is working
dfUpdates = updateCreate()
updateRecords(dfUpdates,0,"N")

# COMMAND ----------

# MAGIC %md
# MAGIC #NO PHOTON

# COMMAND ----------

# DBTITLE 1,Testing update with date range
dfUpdates = updateCreate()
updateRecords(dfUpdates,0,"R")

# COMMAND ----------

# DBTITLE 1,Testing update checking if Dynamic File Prunning is working
dfUpdates = updateCreate()
updateRecords(dfUpdates,0,"N")

# COMMAND ----------


