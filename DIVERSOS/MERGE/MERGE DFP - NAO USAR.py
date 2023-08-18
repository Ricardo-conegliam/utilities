# Databricks notebook source
# MAGIC %pip install Faker

# COMMAND ----------

from delta import DeltaTable
from pyspark.sql.functions import col,lit, sum, avg, count, monotonically_increasing_id, hash, concat, current_timestamp
from datetime import date, timedelta

from pyspark.sql import functions as F
from faker import Faker
from collections import OrderedDict 
import uuid
fake = Faker()
import random

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", False)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", False)

maxIds = 100000000



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.merge_dfp.transactios_cdc (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   transaction_date STRING,
# MAGIC   payment_date STRING,
# MAGIC   amount DOUBLE,
# MAGIC   discount DOUBLE,
# MAGIC   tax DOUBLE,
# MAGIC   shipping DOUBLE,
# MAGIC   item_count DOUBLE,
# MAGIC   operation STRING,
# MAGIC   ean STRING,
# MAGIC   credit_card1 STRING,
# MAGIC   credit_card2 STRING,
# MAGIC   sale_location STRING,
# MAGIC   gps_auto_location STRING,
# MAGIC   near_store STRING,
# MAGIC   operation_date STRING,
# MAGIC   timestamp TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE main.merge_dfp.transactios_cdc SET TBLPROPERTIES ("delta.targetFileSize" = "64MB");

# COMMAND ----------

# now let's collect the min and max date from the streaming dataframe, and use it in our merge statement
from pyspark.sql.functions import min, max

def updateRecordsWithDateRangePredicate(batch_df, batch_id):
    # collects the date ranges in the incoming data frame
    # row = batch_df.select(min("record_date"), max("record_date")).collect()[0]
    # min_date, max_date = [row[i] for i in (0,1)]
    
    # now let's merge with the delta table using the range as a filter predicate
    deltaTable = DeltaTable.forName(spark, "main.merge_DFP.transactios_cdc")

    (deltaTable.alias("t")
         .merge(batch_df.alias("i"),
                "t.id = i.id AND t.transaction_date BETWEEN '{}' AND '{}'".format(min_date, max_date))
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    print ("Query:", "t.id = i.id AND t.transaction_date BETWEEN '{}' AND '{}'".format(min_date, max_date))
    
# updateRecordsWithDateRangePredicate(df_streaming, 0)

# COMMAND ----------

# DBTITLE 1,Main table for merge test
fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
fake_email = F.udf(fake.ascii_company_email)
fake_ean = F.udf(fake.ean)
fake_date = F.udf(lambda:fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S"))
fake_address = F.udf(fake.address)
fake_credit_card_full = F.udf(fake.credit_card_full)
operations = OrderedDict([("APPEND", 0.5),("DELETE", 0.1),("UPDATE", 0.3),(None, 0.01)])
fake_operation = F.udf(lambda:fake.random_elements(elements=operations, length=1)[0])
# fake_id = F.udf(lambda: str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None)


df = spark.range(0, maxIds).repartition(64)
# df = df.withColumn("id", monotonically_increasing_id())
df = df.withColumn("transaction_date", fake_date())
df = df.withColumn("payment_date", fake_date())
df = df.withColumn("amount", F.round(F.rand()*1000))
df = df.withColumn("discount", F.round(F.rand()*10))
df = df.withColumn("tax", F.round(F.rand()*10))
df = df.withColumn("shipping", F.round(F.rand()*10))
df = df.withColumn("item_count", F.round(F.rand()*10))
df = df.withColumn("operation", fake_operation())
df = df.withColumn("ean", fake_ean())
df = df.withColumn("credit_card1",fake_credit_card_full())
df = df.withColumn("sale_location",fake_address())
# df = df.withColumn("gps_auto_location",fake_address())
# df = df.withColumn("near_store",fake_address())
df = df.withColumn("timestamp", current_timestamp())
df = df.drop("id")

df.repartition(128).write.format("delta").mode("append").saveAsTable("main.merge_DFP.transactios_cdc")


  

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(id) from main.merge_dfp.transactios_cdc;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history main.merge_dfp.transactios_cdc;

# COMMAND ----------

# DBTITLE 1,Update fake data dataframe

# fake_id = F.udf(lambda: str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None)


dfUpd = spark.range(500, 1500).repartition(64)
# df = df.withColumn("id", monotonically_increasing_id())
dfUpd = dfUpd.withColumn("transaction_date", fake_date())
dfUpd = dfUpd.withColumn("payment_date", fake_date())
dfUpd = dfUpd.withColumn("amount", F.round(F.rand()*1000))
dfUpd = dfUpd.withColumn("discount", F.round(F.rand()*10))
dfUpd = dfUpd.withColumn("tax", F.round(F.rand()*10))
dfUpd = dfUpd.withColumn("shipping", F.round(F.rand()*10))
dfUpd = dfUpd.withColumn("item_count", F.round(F.rand()*10))
dfUpd = dfUpd.withColumn("operation", lit("UPDATED"))
dfUpd = dfUpd.withColumn("ean", fake_ean())
dfUpd = dfUpd.withColumn("credit_card1",fake_credit_card_full())
dfUpd = dfUpd.withColumn("sale_location",fake_address())
# df = df.withColumn("gps_auto_location",fake_address())
# df = df.withColumn("near_store",fake_address())
dfUpd = dfUpd.withColumn("timestamp", current_timestamp())
# df = df.drop("id")





# COMMAND ----------

dfUpd.count()

# COMMAND ----------

# MAGIC %sql select count(*),max(id),max(timestamp) from main.merge_DFP.transactios_cdc ;

# COMMAND ----------

# %sql
# optimize main.merge_DFP.transactios_cdc zorder by id;

# COMMAND ----------

dfUpdates = spark.read.table("main.merge_DFP.transactios_cdc").where(f"id > {maxIds-100}")

dfUpdates.count()

display(dfUpdates)

# COMMAND ----------

# DBTITLE 1,Checking files sizes (not zordered)
df_stats = spark.read.table("main.schema01.transactios_cdc").withColumn("SourceInfo",col("_metadata"))

df_stats.groupBy("SourceInfo.file_name").sum("SourceInfo.file_size").show()



# COMMAND ----------

# DBTITLE 1,Test queries (Use on DB SQL UI)
# MAGIC %sql
# MAGIC select sum(amount),avg(discount) from main.schema01.transactions;
# MAGIC
# MAGIC select sum(amount),avg(discount) from main.schema01.transactions_optimized;
# MAGIC
# MAGIC select sum(amount),avg(discount) from main.schema01.transactions where transaction_date between "2023-03-02" and "2023-03-03";
# MAGIC
# MAGIC select sum(amount),avg(discount) from main.schema01.transactions_optimized where transaction_date between "2023-03-02" and "2023-03-03";
# MAGIC
# MAGIC select sum(amount),avg(discount) from main.schema01.transactions_zorder where transaction_date between "2023-03-02" and "2023-03-03";

# COMMAND ----------


