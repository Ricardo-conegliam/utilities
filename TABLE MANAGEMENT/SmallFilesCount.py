# Databricks notebook source
from delta import DeltaTable
from datetime import datetime
from pyspark.sql.functions import col, lit, round, current_timestamp, coalesce

# COMMAND ----------

dbutils.widgets.text("Catalog","main")
dbutils.widgets.text("Days_Since_Last_Alt","30")

catalog = dbutils.widgets.get("Catalog")
days_since_last_alt = dbutils.widgets.get("Days_Since_Last_Alt")

## LEGACY MODE IS NOT WORKING, USE ONLY "UC" TYPE
catalogType = "UC"
table_file_stats = "main.default.tablefilestats"
verbose = False

now = datetime.now() 

batch_id = now.strftime("%Y%m%d-%H%M%S")



# COMMAND ----------

spark.sql(f" \
CREATE TABLE IF NOT EXISTS {table_file_stats} ( \
  batchId STRING,\
  catalog STRING, \
  schema STRING, \
  table STRING, \
  numFiles BIGINT, \
  sizeMb DOUBLE, \
  avgFileSizeMb DOUBLE, \
  timestamp TIMESTAMP \
)")

# spark.sql(f"insert into {table_file_stats} ({table['table_catalog']},{table['table_schema']},{table['table_name']},)")

# COMMAND ----------

# Note:  Be aware you are not zordering anything!


def listSmallfiles(catalog):

    ## Note : if you are sadly not using UC, use this dataframe definition instead ##
    if catalogType == "LEGACY":
        df = spark.sql(f"show tables from {catalog}.{schema}").select(
            col("tableName").alias("table_name"),
            lit(schema).alias("table_schema"),
            lit(catalog).alias("table_catalog"),
        )
    else:
        df = (
            spark.table("system.information_schema.tables")
            .select("table_catalog", "table_schema", "table_name")
            .where(f'table_catalog = "{catalog}" ')
            .where("data_source_format = 'DELTA'")
            .where("table_catalog <> '__databricks_internal'")
            .where(f"last_altered > now() - interval {days_since_last_alt} days")
            .orderBy("table_schema")
        )

    tableList = [
        data
        for data in df.select(
            col("table_catalog"), col("table_schema"), col("table_name")
        ).collect()
    ]

    list = []

    for table in tableList:

        fullname = (
            f"{table['table_catalog']}.{table['table_schema']}.{table['table_name']}"
        )

        if verbose:  
            print(f"Checking {fullname}...")

        try:

            dfDetail = ( spark.sql(f"describe detail {fullname}")
            .select(
                lit(f"{batch_id}").alias("batchId"),
                lit(table['table_catalog']).alias("catalog"),
                lit(table['table_schema']).alias("schema"),
                lit(table['table_name']).alias("table"),
                "numFiles",
                round((col("sizeInBytes") / lit(1024) / lit(1024)), 3).alias(
                    "sizeMB"
                ),
                round((col("sizeMB") / col("numFiles")), 3).alias(
                    "avgFileSizeMB"
                ))
                .where("numFiles > 0")
            )

            if dfDetail.count() > 0:
                if verbose:
                    print(f"Appending {fullname}")
                list.append(dfDetail.collect()[0])
        except Exception as e:
            output = f"{e}"
            print(f"Error on {fullname} {e}")

    return list

# COMMAND ----------

tablesStats = listSmallfiles(catalog)


# COMMAND ----------

df = spark.createDataFrame(tablesStats).withColumn("timestamp", current_timestamp())

if verbose:
    display(df.orderBy(col("avgFileSizeMB").asc()))

df.write.mode("append").saveAsTable(table_file_stats)

spark.sql(f"optimize {table_file_stats}")

# COMMAND ----------


