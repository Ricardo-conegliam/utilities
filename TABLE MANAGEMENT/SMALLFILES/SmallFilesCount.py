# Databricks notebook source
# MAGIC %md
# MAGIC ## Objective : Collect small file stats from given catalog (or all)
# MAGIC - To process all the catalogs, just use "*" as parameter
# MAGIC - You can run it daily to get the health evolution
# MAGIC - You can create Queries and Alerts to monitor the smallfiles stats
# MAGIC - This scripts only Describe Details, so its not dangerous.  However, if you use AutoFixOptimize = Y, it will try to optimize all tables with numFiles > 1 and avgFileSizeinMB > 50
# MAGIC - Do not run with AutoFixOptimize="Y" if you have process with updates and merges taking places
# MAGIC - Vacuum verification takes a while (few seconds per table)!  Just set verifyVacuum if you really need it
# MAGIC - Works only with Unit Catalog
# MAGIC - Auto fix only you be seen after next run of verification
# MAGIC #### Author :  SSA Team, Ricardo Conegliam
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports
from delta import DeltaTable
from datetime import datetime
from pyspark.sql.functions import col, lit, round, current_timestamp, coalesce
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, ArrayType, BooleanType, DoubleType, LongType


# COMMAND ----------

# DBTITLE 1,Variables

dbutils.widgets.text("Catalog","*")
dbutils.widgets.text("Days_Since_Last_Alt","9999")
dbutils.widgets.text("AutoFixOptimize","N")
dbutils.widgets.text("verifyVacuum","Y")
dbutils.widgets.text("autoFixVacuum","N")
dbutils.widgets.text("checkZorder","Y")


catalog = dbutils.widgets.get("Catalog")
days_since_last_alt = dbutils.widgets.get("Days_Since_Last_Alt")
AutoFixOptimize = dbutils.widgets.get("AutoFixOptimize")
verifyVacuum = dbutils.widgets.get("verifyVacuum")
autoFixVacuum = dbutils.widgets.get("autoFixVacuum")
checkZorder = dbutils.widgets.get("checkZorder")


table_file_stats = "main.default.tablefilestats"
table_file_stats_hist = "main.default.tablefilestats_hist"

verbose = True

now = datetime.now() 

# It must have only one stats daily
# If you run twice, the script delete the data with same date (batchId) before insert new data
batch_id = now.strftime("%Y%m%d")



# COMMAND ----------

# DBTITLE 1,Creating the tables if they don't exist
spark.sql(f"\
CREATE TABLE IF NOT EXISTS {table_file_stats} ( \
  batchId STRING,\
  catalog STRING, \
  schema STRING, \
  table STRING, \
  numFiles BIGINT, \
  sizeMb DOUBLE, \
  avgFileSizeMb DOUBLE, \
  timestamp TIMESTAMP, \
  vacuum STRING , \
  zorder STRING , \
  zorderby STRING , \
  last_optimize timestamp\
)")

spark.sql(f"\
  CREATE TABLE IF NOT EXISTS {table_file_stats_hist} ( \
  batchId STRING,\
  catalog STRING, \
  schema STRING, \
  table STRING, \
  numFiles BIGINT, \
  sizeMb DOUBLE, \
  avgFileSizeMb DOUBLE, \
  timestamp TIMESTAMP, \
  vacuum STRING, \
  zorder STRING , \
  zorderby STRING, \
  last_optimize timestamp\
)")


schema = StructType(
    [
        StructField("batchId", StringType(), True),
        StructField("catalog", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True),
        StructField("numFiles", LongType(), True),
        StructField("sizeMb", DoubleType(), True),
        StructField("avgFileSizeMb", DoubleType(), True),
        StructField("vacuum", StringType(), True),
        StructField("zorder", StringType(), True),
        StructField("zorderby", StringType(), True),
        StructField("last_optimize", TimestampType(), True)
    ]
)


# COMMAND ----------

# DBTITLE 1,Getting tables from catalog to process
def getTableFromCatalog(catalog):
    # reading tables from catalog 
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

    list = [
        data
        for data in df.select(
            col("table_catalog"), col("table_schema"), col("table_name"), col("last_altered")
        ).collect()
    ]
    return list

# COMMAND ----------

def getTableInfo(ptable):

    if verbose:  
        print(f"        Getting files info... {ptable['table_catalog']}.{ptable['table_schema']}.{ptable['table_name']}")

    dfReturn = ( 
        spark.sql(f"describe detail {ptable['table_catalog']}.{ptable['table_schema']}.{ptable['table_name']}")
        .select(
        lit(f"{batch_id}").alias("batchId"),
        lit(ptable['table_catalog']).alias("catalog"),
        lit(ptable['table_schema']).alias("schema"),
        lit(ptable['table_name']).alias("table"),
        "numFiles",
        round((col("sizeInBytes") / lit(1024) / lit(1024)), 3).alias(
            "sizeMB"
        ),
        round((col("sizeMB") / col("numFiles")), 3).alias(
            "avgFileSizeMB"
        ))
        .where("numFiles > 0")
        )
    

    return dfReturn

# COMMAND ----------

# DBTITLE 1,Main function, where the files, vacuum and zorder are checked
# Note:  Be aware you are not zordering anything!


def listSmallfiles(catalog):


    tableList = getTableFromCatalog(catalog)

    list = []

    for table in tableList:

        fullname = (
            f"{table['table_catalog']}.{table['table_schema']}.{table['table_name']}"
        )

        if verbose:  
            print(f"    Checking {fullname}")

        try:

            dfDetail = getTableInfo (table)

            if verifyVacuum == "Y" or checkZorder == "Y" :

                if verbose: print(f"        Caching history information...")
                dfHistory = spark.sql(f"desc history {fullname}").cache()


            if dfDetail.count() > 0:  ## there is at least one information regarding the table by describe detail
                
                ##  Cheking if last optimize was with zorder
                vacuum = "N/A"
               
                if verifyVacuum == "Y":

                    if verbose: print(f"        Cheking Vaccum...")

                    v_last_altered = table['last_altered']

                    dfVacuum = (dfHistory
                                .where(f"timestamp > '{v_last_altered}' OR '{v_last_altered}' > now() - INTERVAL 7 DAYS")
                                .where("operation = 'VACUUM END'")
                                .where("operationParameters.status='COMPLETED'")
                               )

                    vacuum = "Y"

                    if dfVacuum.count() == 0:

                        if autoFixVacuum == "Y":
                        
                            if verbose: print(f"            Vacuuming...")

                            try:
                                spark.sql(f"VACUUM {fullname}")
                            except Exception as e:  
                                output = f"{e}"  
                                if verbose: print(f"                Error on Vacuun!")
                                vacuum = "E"
                        else:
                            vacuum = "N"
                
                
                ##  Cheking if last optimize was with zorder
                ##  It gets zorder information since the last optimize was done using zorder by
                ##  Only filter last 90 days
                zorder = "N"
                zorderby =  ""
                lastOptimize = None

                if checkZorder == "Y":

                    if verbose: print(f"        Cheking zorder historic...")

                    try:
                        zorderby,lastOptimize =(   
                                    dfHistory
                                    .select('operationParameters.zOrderBy','timestamp')
                                    # .filter("operation='OPTIMIZE' and timestamp > now() - interval 30 days and operationParameters.zOrderBy <> '[]'")
                                    .filter("operation='OPTIMIZE' and timestamp > now() - interval 90 days")
                                    .orderBy(col("timestamp").desc())
                                    .first()
                                )
                        
                        zorderby = zorderby.replace("[","")
                        zorderby = zorderby.replace("]","")
                        zorderby = zorderby.replace('"','')

                        if zorderby != "": zorder="Y"

                    except Exception as e:  
                        output = f"{e}"  
                        print(f"        Error analyzing zorder {fullname} ")

        
                dfDetail = (
                            dfDetail
                            .withColumn("vacuum",lit(vacuum))
                            .withColumn("zorder",lit(zorder))
                            .withColumn("zorderby",lit(zorderby))
                            .withColumn("last_optimize",lit(lastOptimize))
                            )

                if verbose: 
                    print("        Appending metada...")
                list.append(dfDetail.collect()[0])

            if verifyVacuum == "Y" or checkZorder == "Y" :
                dfHistory.unpersist()

        except Exception as e:
            output = f"{e}"
            print(f"        Error on {fullname} {e}")

    return list

# COMMAND ----------

# DBTITLE 1,Optimizing function (for tables with numFiles > 1 and avgFileSize <50MB)
def optimizeTables():
    df = ( 
          spark.sql(f" \
                    select \
                            catalog,schema,table,numFiles,zorder,zorderby \
                    from \
                            {table_file_stats} \
                    where   \
                            (catalog = '{catalog}' OR '{catalog}' = '*') \
                            and \
                            avgFileSizeMb < 10 \
                            and \
                            numFiles > 1 \
                    order by \
                            avgFileSizeMb \
                    ")
          )
          
    tableList = [data for data in df.collect()]

    for table in tableList:

        fullname = f"{table['catalog']}.{table['schema']}.{table['table']}"

        try:
            zorderClause = ""
            if table['zorder'] == "Y":
                zorderFields = table['zorderby']
                zorderClause = f" ZORDER BY {zorderFields}"

            print(f"Running Optimize on {fullname} {zorderClause}...")

            spark.sql(f"OPTIMIZE {fullname} {zorderClause}")

        except Exception as e:  
            output = f"{e}"  
            print(f"    Error on optimizing {fullname} : {e}")
    

# COMMAND ----------

# DBTITLE 1,Saving the data
def writeDataframe(pcatalog,ptablesStats):

    if ptablesStats:

        if verbose:
            print(f"Writing data of catalog {pcatalog}")

        df = spark.createDataFrame(tablesStats,schema).withColumn("timestamp", current_timestamp())

        if verbose:
            print(f"    Deleting old information for {pcatalog}")

        spark.sql(f"DELETE FROM {table_file_stats} WHERE catalog = '{pcatalog}'")
        spark.sql(f"DELETE FROM {table_file_stats_hist} WHERE batchId = '{batch_id}' and catalog = '{pcatalog}'")

        if verbose:
            print(f"    Saving data for {pcatalog}")
            
        df.write.mode("append").saveAsTable(table_file_stats)
        df.write.mode("append").saveAsTable(table_file_stats_hist)
    


# COMMAND ----------

# DBTITLE 1,Optimizing stats function 
def autoClean():
    spark.sql(f"optimize {table_file_stats}")
    spark.sql(f"optimize {table_file_stats_hist}")

    spark.sql(f"vacuum {table_file_stats}")
    spark.sql(f"vacuum {table_file_stats_hist}")

# COMMAND ----------

# DBTITLE 1,Calling the function listSmallfiles to append in list
if catalog == "*":  ## all catalogs will be processed

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

if AutoFixOptimize == "Y":
    optimizeTables()

autoClean()
