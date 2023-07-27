# Databricks notebook source
# MAGIC %sql 
# MAGIC
# MAGIC use main.schema01;
# MAGIC
# MAGIC -- reset demo
# MAGIC truncate table main.schema01.g_final;
# MAGIC truncate table main.schema01.g_jobs;
# MAGIC truncate table main.schema01.g_temps;
# MAGIC truncate table main.schema01.g_chems;
# MAGIC
# MAGIC

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", False)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", False)

spark.conf.set("spark.sql.shuffle.partitions","8")

# COMMAND ----------

dbutils.fs.rm("/tmp/checkpoint",True)

# COMMAND ----------

# Gera dados sintéticos para o teste
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType

def add_data(month,pdate,jobOnly):
    # Sample data for the tables
    # date_modifier = str(modifier)[0]
    # month = pmonth.rjust(2,"0")

    job_data = [
        ("job_id", "job_start", "job_end"),
        (month*100+pdate+10000, f"2023-{month}-{pdate} 10:00:00", "2023-05-15 11:00:00"),
        (month*100+pdate+20000, f"2023-{month}-{pdate} 10:00:00", "2023-05-15 13:00:00"),
        (month*100+pdate+30000, f"2023-{month}-{pdate} 10:00:00", "2023-05-15 15:00:00"),
    ]

    job_df = spark.createDataFrame(job_data[1:],schema=job_data[0]).withColumn('job_start',col("job_start").cast(TimestampType()))
    job_df.write.mode("append").saveAsTable("main.schema01.g_jobs")

    if not jobOnly:    

        temperature_data = [
            ("job_id", "temp_check", "temp_number"),
            (month*100+pdate+10000, f"2023-{month}-15 10:05:00", 25.0),
            (month*100+pdate+20000, f"2023-{month}-15 12:05:00", 24.5),
            (month*100+pdate+30000, f"2023-{month}-15 14:05:00", 27.2),
        ]

        chemical_data = [
            ("job_id", "chemical_name", "amount"),
            (month*100+pdate+10000, "Chemical A", 10.0),
            (month*100+pdate+20000, "Chemical B", 12.0),
            (month*100+pdate+30000, "Chemical A", 9.5),
        ]

        temp_df = spark.createDataFrame(temperature_data[1:],schema=temperature_data[0])
        chem_df = spark.createDataFrame(chemical_data[1:],schema=chemical_data[0])

        temp_df.write.mode("append").saveAsTable("main.schema01.g_temps")
        chem_df.write.mode("append").saveAsTable("main.schema01.g_chems")

for m in range(3,4):
    for d in range(1,30):
        add_data(m,d,False)

for d in range(1,23):
    add_data(5,d,False)
#display(job_df)

# COMMAND ----------

# MAGIC %sql select count(*) from g_jobs

# COMMAND ----------

# MAGIC %sql desc detail main.schema01.g_jobs
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from delta import DeltaTable

def merge_all(batch, batch_id):
    
    update = (batch
                .join(spark.table("main.schema01.g_temps"),"job_id","inner")
                .join(spark.table("main.schema01.g_chems"),"job_id","inner"))
    

    merging = (DeltaTable.forName(spark,"main.schema01.g_final").merge(
                                        source = update.alias("update"),
                                        condition = expr("g_final.job_id = update.job_id and g_final.job_start > current_date() - INTERVAL 2 DAYS ")
                                        ).whenMatchedUpdateAll()
                                        .whenNotMatchedInsertAll()
                                        .execute())

# COMMAND ----------

job_stream = (spark.readStream
                   .table("main.schema01.g_jobs")
                   .writeStream
                   .option("checkpointLocation", "/tmp/checkpoint")
                   .foreachBatch(merge_all)
                   .trigger(once=True)
                   .start())

# COMMAND ----------

add_data(5,23,True)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from main.schema01.g_jobs
# MAGIC where job_start > current_date() - INTERVAL 2 DAYS

# COMMAND ----------

# MAGIC %sql desc detail main.schema01.g_final

# COMMAND ----------

job_stream_test = (spark.readStream.option("skipChangeCommits", "true")
                   .table("main.schema01.g_final").start())

# COMMAND ----------


