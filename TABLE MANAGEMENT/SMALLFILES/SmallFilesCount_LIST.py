# Databricks notebook source
from delta import DeltaTable
from pyspark.sql.functions import col,lit,round

# COMMAND ----------

catalog = dbutils.widgets.text("Catalog","main")

## LEGACY MODE IS NOT WORKING, USE ONLY "UC" TYPE
catalogType = "UC"
table_file_stats = "main.default.TABLE_FILE_STATS"


# COMMAND ----------

# Note:  Be aware you are not zordering anything!


def listSmallfiles(catalog, schema):

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

        print(f"Checking {fullname}...")
        try:
            list.append(
                spark.sql(f"describe detail {fullname}")
                .select(
                    "name",
                    "numFiles",
                    round((col("sizeInBytes") / lit(1024) / lit(1024)), 3).alias(
                        "sizeInMB"
                    ),
                    round((col("sizeInMB") / col("numFiles")), 3).alias(
                        "avgFileSizeMB"
                    ),
                # 
                .where("numFiles > 0")
                .collect()[0]
            )
        except Exception as e:
            output = f"{e}"
            print(f"Error on {fullname} {e}")

    return list

# COMMAND ----------

tablesStats = listSmallfiles(catalog,schema)


# COMMAND ----------

df = spark.createDataFrame(tablesStats)
display(df.orderBy(col("avgFileSizeMB").asc()))

# COMMAND ----------


