# Databricks notebook source
# MAGIC %pip install Faker

# COMMAND ----------

from delta import DeltaTable
from pyspark.sql.functions import col,lit, sum, avg, count
from datetime import date, timedelta

from pyspark.sql import functions as F
from faker import Faker
from collections import OrderedDict 
import uuid
fake = Faker()
import random


# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", False)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", False)

# COMMAND ----------

# %sql
# alter table main.schema01.transactions set tblproperties 
#    (
#     delta.autoOptimize.optimizeWrite = False, 
#     delta.autoOptimize.autoCompact = False
#  );

#  truncate table main.schema01.transactions;

# COMMAND ----------

folder = "/demos/ss/cdc_raw"
#dbutils.fs.rm(folder, True)


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
fake_id = F.udf(lambda: str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None)

df = spark.range(0, 10000).repartition(16)
df = df.withColumn("id", fake_id())
df = df.withColumn("firstname", fake_firstname())
df = df.withColumn("lastname", fake_lastname())
df = df.withColumn("email", fake_email())
df = df.withColumn("address", fake_address())
df = df.withColumn("operation", fake_operation())
df_customers = df.withColumn("operation_date", fake_date())


#   df_customers.repartition(100).write.format("json").mode("overwrite").save(folder+"/customers")
df_customers.repartition(1).write.option("mergeSchema", "true").format("delta").mode("overwrite").saveAsTable("main.schema01.customers")

df = spark.range(0, 10000000).repartition(64)
df = df.withColumn("id", fake_id())
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
df = df.withColumn("credit_card2",fake_credit_card_full())
df = df.withColumn("sale_location",fake_address())
df = df.withColumn("gps_auto_location",fake_address())
df = df.withColumn("near_store",fake_address())
df = df.withColumn("operation_date", fake_date())

#Join with the customer to get the same IDs generated.

#   df = df.withColumn("t_id", F.monotonically_increasing_id()).join(spark.read.json(folder+"/customers").select("id").withColumnRenamed("id", "customer_id").withColumn("t_id", F.monotonically_increasing_id()), "t_id").drop("t_id")
#   df.repartition(10).write.format("json").mode("overwrite").save(folder+"/transactions")


df = df.withColumn("t_id", F.monotonically_increasing_id()).join(spark.read.table("main.schema01.customers").select("id").withColumnRenamed("id", "customer_id").withColumn("t_id", F.monotonically_increasing_id()), "t_id").drop("t_id")
df.write.option("mergeSchema", "true").format("delta").mode("overwrite").saveAsTable("main.schema01.transactions")

  

# COMMAND ----------

# DBTITLE 1,Checking files sizes (not zordered)
df_stats = spark.read.table("main.schema01.transactions").withColumn("SourceInfo",col("_metadata"))

df_stats.groupBy("SourceInfo.file_name").sum("SourceInfo.file_size").show()



# COMMAND ----------

# DBTITLE 1,Making the table larger
for _ in range(10):
    df = spark.read.table("main.schema01.transactions")
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
    df = df.withColumn("credit_card2",fake_credit_card_full())
    df = df.withColumn("sale_location",fake_address())
    df = df.withColumn("gps_auto_location",fake_address())
    df = df.withColumn("near_store",fake_address())
    df = df.withColumn("operation_date", fake_date())

    df.repartition(16).write.mode("append").saveAsTable("main.schema01.transactions")

# COMMAND ----------

df_stats = spark.read.table("main.schema01.transactions").withColumn("SourceInfo",col("_metadata"))

df_stats.groupBy("SourceInfo.file_name").count().show()


# COMMAND ----------

# DBTITLE 1,Cloning and zordering
# MAGIC %sql
# MAGIC
# MAGIC set spark.databricks.delta.optimize.maxFileSize = 1073741824;
# MAGIC
# MAGIC drop table if exists main.schema01.transactions_optimized;
# MAGIC create table main.schema01.transactions_optimized deep clone main.schema01.transactions;
# MAGIC optimize main.schema01.transactions_optimized;
# MAGIC
# MAGIC

# COMMAND ----------

set spark.databricks.delta.optimize.maxFileSize = 67108864;
drop table if exists main.schema01.transactions_zorder;
create table main.schema01.transactions_zorder deep clone main.schema01.transactions;

optimize main.schema01.transactions_zorder zorder by (transaction_date);

# COMMAND ----------

# DBTITLE 1,Checking file sizes of transactions zordered
df_stats = spark.read.table("main.schema01.transactions_zorder").withColumn("SourceInfo",col("_metadata"))

df_stats.groupBy("SourceInfo.file_name").sum("SourceInfo.file_size").show()

# COMMAND ----------

# start_date = date(2022, 1, 1)
# end_date = date(2023, 7, 1)
# delta = timedelta(days=1)


# spark.sql("truncate table main.schema01.events_large")
# while start_date <= end_date:

#     data_string = start_date.strftime("%Y-%m-%d")

#     print(data_string)
#     start_date += delta

#     for customer in range(100,150):
    
#         spark.sql(f"insert into main.schema01.events_large select '{data_string}' event_time, '{customer}' customer, * except(event_time,customer) from main.schema01.events;")

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

# MAGIC %sql
# MAGIC
# MAGIC create or replace table main.schema01.transactions_clustered cluster by (transaction_date) 
# MAGIC TBLPROPERTIES (delta.autoOptimize.optimizeWrite = False, delta.targetFileSize = "10000") 
# MAGIC as select * from main.schema01.transactions;
# MAGIC
# MAGIC -- ALTER TABLE main.schema01.transactions_clustered set TBLPROPERTIES (delta.autoOptimize.optimizeWrite = False, delta.targetFileSize = "1073741824") ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES main.schema01.transactions_clustered

# COMMAND ----------

# MAGIC %sql
# MAGIC -- set spark.databricks.delta.optimize.maxFileSize = 1073741824;
# MAGIC optimize main.schema01.transactions_clustered;
# MAGIC

# COMMAND ----------


