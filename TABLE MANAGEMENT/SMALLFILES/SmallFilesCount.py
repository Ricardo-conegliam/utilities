# Databricks notebook source
# MAGIC %md
# MAGIC ## Objective : Collect small file stats from given catalog (or all)
# MAGIC - To process all the catalogs, just use "*" as parameter
# MAGIC - You can run it daily to get the health evolution
# MAGIC - You can create Queries and Alerts to monitor the smallfiles stats
# MAGIC - This scripts only Describe Details, so its not dangerous.  However, if you use AutoFixOptimize = Y, it will try to optimize all tables with numFiles > 1 and avgFileSizeinMB > 50
# MAGIC - Do not run with AutoFixOptimize="Y" if you have process with updates and merges taking places
# MAGIC - Vacuum verification takes a while (few seconds per table)!  Just set VerifyVacuum if you really need it
# MAGIC - Works only with Unit Catalog
# MAGIC - Auto fix only you be seen after next run of verification
# MAGIC #### Author :  SSA Team, Ricardo Conegliam
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports
from delta import DeltaTable
from datetime import datetime
from pyspark.sql.functions import col, lit, round, current_timestamp, coalesce

# COMMAND ----------

# DBTITLE 1,Variables

dbutils.widgets.text("Catalog","main")
dbutils.widgets.text("Days_Since_Last_Alt","9999")
dbutils.widgets.text("AutoFixOptimize","N")
dbutils.widgets.text("VerifyVacuum","N")
dbutils.widgets.text("AutoFixVacuum","N")


catalog = dbutils.widgets.get("Catalog")
days_since_last_alt = dbutils.widgets.get("Days_Since_Last_Alt")
AutoFixOptimize = dbutils.widgets.get("AutoFixOptimize")
VerifyVacuum = dbutils.widgets.get("VerifyVacuum")
AutoFixVacuum = dbutils.widgets.get("AutoFixVacuum")

table_file_stats = "main.default.tablefilestats"
table_file_stats_hist = "main.default.tablefilestats_hist"

verbose = True

now = datetime.now() 

# It must have only one stats daily
# If you run twice, the script delete the data with same date (batchId) before insert new data
batch_id = now.strftime("%Y%m%d")



# COMMAND ----------

# DBTITLE 1,Creating the tables if they don't exist
spark.sql(f" \
CREATE TABLE IF NOT EXISTS {table_file_stats} ( \
  batchId STRING,\
  catalog STRING, \
  schema STRING, \
  table STRING, \
  numFiles BIGINT, \
  sizeMb DOUBLE, \
  avgFileSizeMb DOUBLE, \
  timestamp TIMESTAMP, \
  vacuum STRING \
)")

spark.sql(f" \
CREATE TABLE IF NOT EXISTS {table_file_stats_hist} ( \
  batchId STRING,\
  catalog STRING, \
  schema STRING, \
  table STRING, \
  numFiles BIGINT, \
  sizeMb DOUBLE, \
  avgFileSizeMb DOUBLE, \
  timestamp TIMESTAMP, \
  vacuum STRING \
)")


# COMMAND ----------

# DBTITLE 1,Creating a function 
# Note:  Be aware you are not zordering anything!


def listSmallfiles(catalog):

    df = (
        spark.table("system.information_schema.tables")
        .select("table_catalog", "table_schema", "table_name","last_altered")
        .where(f'table_catalog = "{catalog}" ')
        .where("table_catalog <> 'information_schema'")
        .where("data_source_format = 'DELTA'")
        .where("table_catalog <> '__databricks_internal'")
        .where(f"last_altered > now() - interval {days_since_last_alt} days")
        .orderBy("table_schema")
    )

    tableList = [
        data
        for data in df.select(
            col("table_catalog"), col("table_schema"), col("table_name"), col("last_altered")
        ).collect()
    ]

    list = []

    for table in tableList:

        fullname = (
            f"{table['table_catalog']}.{table['table_schema']}.{table['table_name']}"
        )

        if verbose:  
            print(f"    Checking {fullname}...")
            print(f"        Getting files info...")

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

                vacuum = "N/A"
               
                if VerifyVacuum == "Y":

                    if verbose:
                        print(f"        Cheking Vaccum...")

                    v_last_altered = table['last_altered']

                    dfVacuum = (spark
                                .sql(f"desc history {fullname}")
                                .where(f"timestamp > '{v_last_altered}' OR '{v_last_altered}' > now() - INTERVAL 7 DAYS")
                                .where("operation = 'VACUUM END'")
                                .where("operationParameters.status='COMPLETED'")
                            )

                    vacuum = "Y"

                    if dfVacuum.count() == 0:

                        if AutoFixVacuum == "Y":
                        
                            print(f"            Vacuuming...")

                            try:
                                spark.sql(f"VACUUM {fullname}")
                            except Exception as e:  
                                output = f"{e}"  
                                print(f"                Error on Vacuun!")
                                vacuum = "E"
                        else:
                            vacuum = "N"

                        
                    dfDetail = dfDetail.withColumn("vacuum",lit(vacuum))

                    if verbose:
                        print(f"        Appending metada...")
                    list.append(dfDetail.collect()[0])

        except Exception as e:
            output = f"{e}"
            print(f"        Error on {fullname} {e}")

    return list

# COMMAND ----------

# DBTITLE 1,Saving the data
def writeDataframe(pcatalog,ptablesStats):

    if ptablesStats:

        if verbose:
            print(f"Writing data of catalog {pcatalog}")

        df = spark.createDataFrame(tablesStats).withColumn("timestamp", current_timestamp())

        if verbose:
            display(df.orderBy(col("avgFileSizeMB").asc()))


        spark.sql(f"DELETE FROM {table_file_stats} WHERE catalog = '{pcatalog}'")
        spark.sql(f"DELETE FROM {table_file_stats_hist} WHERE batchId = '{batch_id}' and catalog = '{pcatalog}'")


        df.write.mode("append").saveAsTable(table_file_stats)
        df.write.mode("append").saveAsTable(table_file_stats_hist)
    


# COMMAND ----------

# DBTITLE 1,Calling the function listSmallfiles to append in list
if catalog == "*":

    catalogs = (
        spark.sql("SELECT catalog_name FROM system.information_schema.catalogs WHERE catalog_name <> 'system'")
        .collect()
    )

    for catalog_to_analyze in catalogs:
        if verbose:
            print(f"Analyzing catalog {catalog_to_analyze['catalog_name']}")
        tablesStats = listSmallfiles(catalog_to_analyze['catalog_name'])
        writeDataframe(catalog_to_analyze['catalog_name'],tablesStats)

else:
    tablesStats = listSmallfiles(catalog)
    writeDataframe(catalog,tablesStats)

# COMMAND ----------

# DBTITLE 1,Optimizing stats tables
spark.sql(f"optimize {table_file_stats}")
spark.sql(f"optimize {table_file_stats_hist}")

spark.sql(f"vacuum {table_file_stats}")
spark.sql(f"vacuum {table_file_stats_hist}")

# COMMAND ----------

# DBTITLE 1,Optimizing tables with numFiles > 1 and avgFileSize <50MB
if AutoFixOptimize == "Y":
    df = ( 
          spark.sql(f" \
                    select \
                            catalog,schema,table,numFiles \
                    from \
                            {table_file_stats} \
                    where   \
                            (catalog = '{catalog}' OR '{catalog}' = '*') \
                            and \
                            avgFileSizeMb < 50 \
                            and \
                            numFiles > 1 \
                    order by \
                            avgFileSizeMb \
                    ")
          )
          
    tableList = [data for data in df.collect()]

    for table in tableList:

        fullname = f"{table['catalog']}.{table['schema']}.{table['table']}"

        print(f"Running Optimize on {fullname}...")

        try:
            df = DeltaTable.forName(spark,f"{fullname}")
            df.optimize().executeCompaction()

        except Exception as e:  
            output = f"{e}"  
            print(f"    Error on optimizing {fullname} : {e}")
    

# COMMAND ----------

# DBTITLE 1,Vacuuming tables with more than 7 days without it
# if AutoFixVacuum == "Y" and VerifyVacuum == "Y":

    
#     df = spark.sql(f" \
#                     select \
#                             catalog,schema,table \
#                     from \
#                             {table_file_stats} \
#                     where   \
#                             (catalog = '{catalog}' OR '{catalog}' = '*') \
#                             and \
#                             vacuum = 'N' \
#                   ")


#     tableList = [data for data in df.collect()]


#     for table in tableList:

#         fullname = f"{table['catalog']}.{table['schema']}.{table['table']}"

#         print(f"Running Vacuum on {fullname}...")
#         try:
#             spark.sql(f"VACUUM {fullname}")
#         except Exception as e:  
#             output = f"{e}"  
#             print(f"    Error on Vacuun {fullname} ")


# COMMAND ----------


