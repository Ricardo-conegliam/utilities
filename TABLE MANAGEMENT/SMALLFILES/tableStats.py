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
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports
from delta import DeltaTable
from datetime import datetime
from pyspark.sql.functions import col, lit, round, current_timestamp, coalesce, try_divide, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, ArrayType, BooleanType, DoubleType, LongType
from datetime import date, timedelta
import concurrent.futures



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

oneweekbehind = date.today() - timedelta(days=7)
str_oneweekbehind = oneweekbehind.strftime("%Y-%m-%d") 




# It must have only one stats daily
# If you run twice, the script delete the data with same date (batchId) before insert new data
batch_id = now.strftime("%Y%m%d")

# main_list = []



# COMMAND ----------

# DBTITLE 1,Creating the tables if they don't exist
spark.sql(f"\
CREATE TABLE IF NOT EXISTS {table_file_stats} ( \
  batchId STRING,\
  catalog STRING, \
  schema STRING, \
  table STRING, \
  partitionColumns STRING, \
  numFiles BIGINT, \
  sizeMb DOUBLE, \
  avgFileSizeMb DOUBLE, \
  timestamp TIMESTAMP, \
  vacuum STRING , \
  optimize STRING , \
  zorder STRING , \
  zorderby STRING , \
  last_optimize timestamp, \
  lastModified timestamp \
)")

spark.sql(f"\
  CREATE TABLE IF NOT EXISTS {table_file_stats_hist} ( \
  batchId STRING,\
  catalog STRING, \
  schema STRING, \
  table STRING, \
  partitionColumns STRING, \
  numFiles BIGINT, \
  sizeMb DOUBLE, \
  avgFileSizeMb DOUBLE, \
  timestamp TIMESTAMP, \
  vacuum STRING, \
  optimize STRING , \
  zorder STRING , \
  zorderby STRING, \
  last_optimize timestamp,\
  lastModified timestamp \
)")


schema = StructType(
    [
        StructField("batchId", StringType(), True),
        StructField("catalog", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True),
        StructField("partitionColumns", StringType(), True),
        StructField("numFiles", LongType(), True),
        StructField("sizeMb", DoubleType(), True),
        StructField("avgFileSizeMb", DoubleType(), True),
        StructField("vacuum", StringType(), True),
        StructField("zorder", StringType(), True),
        StructField("zorderby", StringType(), True),
        StructField("last_optimize", TimestampType(), True),
        StructField("lastModified", TimestampType(), True)
    ]
)


# COMMAND ----------

# DBTITLE 1,Getting tables from catalog to process
def getTableListFromCatalog(catalog):
    # reading tables from catalog 

    if verbose:  
        print(f"Reading catalog {catalog}")
              
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

    return_list = [
        data
        for data in df.select(
            col("table_catalog"), col("table_schema"), col("table_name"), col("last_altered")
        ).collect()
    ]
    return return_list

# COMMAND ----------

list = getTableListFromCatalog("main")

schemas = [ ]
for row in list:
    if row['table_schema'] not in schemas:
        schemas.append(row['table_schema'])

# COMMAND ----------

def getTableInfo(ptable):

    if verbose:  
        print(f"{ptable['table_catalog']}.{ptable['table_schema']}.{ptable['table_name']} - Getting files info...")

    # TODO
    #try: 
    #except   

    dfReturn = ( 
        spark.sql(f"describe detail {ptable['table_catalog']}.{ptable['table_schema']}.{ptable['table_name']}")
        .select(
        lit(f"{batch_id}").alias("batchId"),
        lit(ptable['table_catalog']).alias("catalog"),
        lit(ptable['table_schema']).alias("schema"),
        lit(ptable['table_name']).alias("table"),
        "lastModified",
        "partitionColumns",
        "numFiles",
        round(try_divide(try_divide(col("sizeInBytes") , lit(1024)) , lit(1024)), 3).alias(
            "sizeMB"
        ),
        round(try_divide(col("sizeMB") , col("numFiles")), 3).alias(
            "avgFileSizeMB"
        ))
        .where("numFiles > 0")
        )
    

    return dfReturn

# COMMAND ----------

# DBTITLE 1,Main function, where the files, vacuum and zorder are checked
# Note:  Be aware you are not zordering anything!
# # Parallelism when syncing the tables
import concurrent.futures, os
default_parallelism = os.cpu_count()



def processCatalog(catalog):


    tableList = getTableListFromCatalog(catalog)

    ## list will be used later
    # list = []

    # for table in tableList:
    #     processTable(table)

    with concurrent.futures.ThreadPoolExecutor(max_workers = default_parallelism) as executor:
        executor.map(processTable, tableList)

 

# COMMAND ----------

def processTable(table):

    fullname = f"{table['table_catalog']}.{table['table_schema']}.{table['table_name']}"

    if verbose:  
        print(f"{fullname}")

    try:

        dfDetail = getTableInfo (table)

        if verifyVacuum == "Y" or checkZorder == "Y" :

            if verbose: print(f"{fullname} - Getting history information...")
            dfHistory = spark.sql(f"desc history {fullname}")   # .cache(). incompatible with serverless
            historyCount = dfHistory.count()

            if verbose: print(f"{fullname} - Versions found : {historyCount}")


        if dfDetail.count() > 0:  ## there is at least one information regarding the table by describe detail
            
            ##  Cheking if last optimize was with zorder
            vacuum = "N/A"


            #Vaccum check and run if necessary

            if verifyVacuum == "Y":

                if verbose: print(f"{fullname} - Cheking Vaccum...")

                # v_last_altered = table['last_altered']
                v_last_altered = dfDetail.select("lastModified").collect()[0][0].strftime("%Y-%m-%d")

                dfVacuum = (dfHistory
                            .where(f"timestamp < '{v_last_altered}' ")
                            .where("operation = 'VACUUM END'")
                            .where("operationParameters.status='COMPLETED'")
                            )


                vacuum = "N"
                vacuum_count = dfVacuum.count()

                # There is not vacuum on table history or it was more the a week ago
                if autoFixVacuum == "Y" and (vacuum_count == 0 or ( vacuum_count > 0 and '{v_last_altered}' < str_oneweekbehind )) :
                    vacuum = vacuum_table(fullname)
           
            
            ##  Cheking if last optimize was with zorder
            ##  It gets zorder information since the last optimize was done using zorder by
            ##  Only filter last 90 days
            zorder = "N"
            zorderby =  ""
            lastOptimize = None
            optimize = "N"

            if checkZorder == "Y":

                if verbose: print(f"{fullname} - Cheking zorder historic...")

                try:
                    zorderby,lastOptimize =(   
                                dfHistory
                                .select('operationParameters.zOrderBy','timestamp')
                                # .filter("operation='OPTIMIZE' and timestamp > now() - interval 30 days and operationParameters.zOrderBy <> '[]'")
                                .filter("operation='OPTIMIZE'")
                                .orderBy(col("timestamp").desc())
                                .first()
                            )
                    
                    zorderby = zorderby.replace("[","")
                    zorderby = zorderby.replace("]","")
                    zorderby = zorderby.replace('"','')

                    if zorderby != "": zorder="Y"

                except Exception as e:  
                    output = f"{e}"  
                    print(f"{fullname} - Error analyzing zorder {fullname} ")
                
                if AutoFixOptimize == "Y" and dfDetail.select("numFiles").collect()[0][0] > 1:
                    optimize = optimize_table(fullname,zorderby)

    
            dfDetail = (
                        dfDetail
                        .withColumn("vacuum",lit(vacuum))
                        .withColumn("optimize",lit(optimize))
                        .withColumn("zorder",lit(zorder))
                        .withColumn("zorderby",lit(zorderby))
                        .withColumn("last_optimize",lit(lastOptimize))
                        )

            if verbose: print(f"{fullname} - Writing metadata...")

            # " ".join(str(x) for x in xs)

            dfDetail.show()

            dfDetail_write = (
                dfDetail
                .withColumn( "partitionColumns", concat_ws(",",col("partitionColumns")) )
                .withColumn( "timestamp" , current_timestamp())
            )


            dfDetail_write.write.mode("append").option("mergeSchema",True).format("delta").saveAsTable(table_file_stats) 
            dfDetail_write.write.mode("append").option("mergeSchema",True).format("delta").saveAsTable(table_file_stats_hist) 

            # # spark.sql(f"INSERT INTO {table_file_stats} ()")

            # main_list
            # main_list.append(dfDetail.collect()[0])

        # removed since it is not working with serverless
        # if verifyVacuum == "Y" or checkZorder == "Y" :
        #     dfHistory.unpersist()

    except Exception as e:
        output = f"{e}"
        print(f"{fullname} - Error on {fullname} {e}")

    return list

# COMMAND ----------

def optimize_table ( fullname, zorderby):

    try:

        print(f"{fullname} {zorderby} - Running Optimize ...")

        spark.sql(f"OPTIMIZE {fullname} {zorderby}")

        return "Y"

    except Exception as e:  
        output = f"{e}"  
        print(f"    Error on optimizing {fullname} : {e}")

        return "E"

# COMMAND ----------

def vacuum_table ( fullname):

    try:

        print(f"{fullname} - Running Vacuum ...")

        spark.sql(f"VACUUM {fullname}")

        return "Y"

    except Exception as e:  
        output = f"{e}"  
        print(f"    Error vacuuming {fullname} : {e}")

        return "E"

# COMMAND ----------

# DBTITLE 1,Saving the data
def writeDataframe(pcatalog,ptablesStats):

    if ptablesStats:

        if verbose:
            print(f"Writing data of catalog {pcatalog}")

        df = spark.createDataFrame(tablesStats,schema).withColumn("timestamp", current_timestamp())

            
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
            print(f"Deleting some metadata for {catalog_to_analyze['catalog_name']}")
        
        spark.sql(f"DELETE FROM {table_file_stats} WHERE catalog = '{catalog_to_analyze['catalog_name']}'")
        spark.sql(f"DELETE FROM {table_file_stats_hist} WHERE batchId = '{batch_id}' and catalog = '{catalog_to_analyze['catalog_name']}'")

        if verbose:
            print(f"Analyzing catalog {catalog_to_analyze['catalog_name']}")
            
        tablesStats = processCatalog(catalog_to_analyze['catalog_name'])
        # writeDataframe(catalog_to_analyze['catalog_name'],tablesStats)

else:

    spark.sql(f"DELETE FROM {table_file_stats} WHERE catalog = '{catalog}'")
    spark.sql(f"DELETE FROM {table_file_stats_hist} WHERE batchId = '{batch_id}' and catalog = '{catalog}'")
    
    tablesStats = processCatalog(catalog)


# COMMAND ----------

dbutils.fs.ls(spark.sql(f"describe detail {table_file_stats}").first()["location"])

# COMMAND ----------


