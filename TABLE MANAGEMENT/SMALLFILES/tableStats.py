# Databricks notebook source
# MAGIC %md
# MAGIC ## Use Predictive Optimization instead of this script - PO still don't do zorder, but its on the way to optimize Liquid Clustering.
# MAGIC
# MAGIC ## Objective : Collect small file stats from given catalog (or all)
# MAGIC - To process all the catalogs, just use "*" as parameter
# MAGIC - You can run it daily to get the health evolution
# MAGIC - You can create Queries and Alerts to monitor the smallfiles stats
# MAGIC - This scripts only Describe Details, so its not dangerous.  However, if you use AutoFixOptimize = Y, it will try to optimize all tables with numFiles > 1 and avgFileSizeinMB > 50
# MAGIC - Do not run with AutoFixOptimize="Y" if you have process with updates and merges taking places
# MAGIC - Vacuum verification takes a while (few seconds per table)!  Just set verifyVacuum if you really need it
# MAGIC - Works only with Unit Catalog
# MAGIC - Auto fix only you be seen after next run of verification
# MAGIC
# MAGIC
# MAGIC Make sure you have all grants needed:
# MAGIC
# MAGIC select concat("grant use catalog on CATALOG `",catalog_name,"` to `93709224-9a7c-4483-b6d7-c7685606ca40`;") from system.information_schema.catalogs
# MAGIC
# MAGIC select concat("grant USE SCHEMA ON CATALOG `",catalog_name,"` to `93709224-9a7c-4483-b6d7-c7685606ca40`;") from system.information_schema.catalogs
# MAGIC
# MAGIC select concat("grant SELECT ON CATALOG `",catalog_name,"` to `93709224-9a7c-4483-b6d7-c7685606ca40`;") from system.information_schema.catalogs
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### Author :  SSA Team, Ricardo Conegliam
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports
# Importa DeltaTable para operações com tabelas Delta
from delta import DeltaTable



# Importa datetime para manipulação de datas e horas
from datetime import datetime

# Importa funções do PySpark para manipulação de DataFrames
from pyspark.sql.functions import col, lit, round, current_timestamp, coalesce, try_divide, concat_ws

# Importa tipos de dados do PySpark para definição de schemas
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, ArrayType, BooleanType, DoubleType, LongType

# Importa date e timedelta para operações com datas
from datetime import date, timedelta

# Importa concurrent.futures para execução paralela
import concurrent.futures

# Importa StorageLevel para definir o nível de persistência de DataFrames
from pyspark.storagelevel import StorageLevel

# COMMAND ----------

# DBTITLE 1,Variables
# Cria widgets para entrada de parâmetros
dbutils.widgets.text("Catalog","*")
dbutils.widgets.text("Days_Since_Last_Alt","9999")
dbutils.widgets.text("AutoFixOptimize","N")
dbutils.widgets.text("verifyVacuum","Y")
dbutils.widgets.text("autoFixVacuum","N")
dbutils.widgets.text("checkZorder","N")

# Obtém os valores dos widgets
catalog = dbutils.widgets.get("Catalog")
days_since_last_alt = dbutils.widgets.get("Days_Since_Last_Alt")
AutoFixOptimize = dbutils.widgets.get("AutoFixOptimize")
verifyVacuum = dbutils.widgets.get("verifyVacuum")
autoFixVacuum = dbutils.widgets.get("autoFixVacuum")
checkZorder = dbutils.widgets.get("checkZorder")

# Define os nomes das tabelas
table_file_stats = "main.default.tablefilestats"
table_file_stats_hist = "main.default.tablefilestats_hist"

# Define a variável de controle de verbosidade
verbose = True

# Obtém a data e hora atual
now = datetime.now() 

# Calcula a data de uma semana atrás
oneweekbehind = date.today() - timedelta(days=7)
str_oneweekbehind = oneweekbehind.strftime("%Y-%m-%d") 

# Gera um ID de lote baseado na data atual
batch_id = now.strftime("%Y%m%d")

# Lê a tabela de sistema com informações sobre outras tabelas
df_system_table = (
        spark.table("system.information_schema.tables")
        .select("table_catalog", "table_schema", "table_name","last_altered","table_type","table_owner")
        .where("table_catalog <> 'information_schema'")
        .where("data_source_format = 'DELTA'")
        .where("table_catalog <> '__databricks_internal'")
        .orderBy("table_schema")
)

# COMMAND ----------

# DBTITLE 1,Creating the tables if they don't exist
# Cria a tabela table_file_stats se não existir
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
  clusteringColumns STRING, \
  clusterByAuto boolean, \
  table_owner STRING, \
  lastModified timestamp \
)")


# COMMAND ----------

# Cria a tabela table_file_stats_hist se não existir
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_file_stats_hist} (
  version BIGINT,
  timestamp TIMESTAMP,
  userId STRING,
  userName STRING,
  operation STRING,
  operationParameters MAP<STRING, STRING>,
  job STRUCT<jobId: STRING, jobName: STRING, jobRunId: STRING, runId: STRING, jobOwnerId: STRING, triggerType: STRING>,
  notebook STRUCT<notebookId: STRING>,
  clusterId STRING,
  readVersion BIGINT,
  isolationLevel STRING,
  isBlindAppend BOOLEAN,
  operationMetrics MAP<STRING, STRING>,
  userMetadata STRING,
  engineInfo STRING,
  clean_fullname STRING)
USING delta
CLUSTER BY (clean_fullname)
""")

# Define o schema para a tabela table_file_stats
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
        StructField("lastModified", TimestampType(), True),
        StructField("table_owner", StringType(), True),
        StructField("table_type", StringType(), True),
        StructField("clusterByAuto", BooleanType(), True),
        StructField("clusteringColumns", StringType(), True)
    ]
)

# COMMAND ----------

# Trunca a tabela table_file_stats_hist que armazena dados do historico temporariamente (colocado por questoes de performance)
spark.sql(f"TRUNCATE TABLE {table_file_stats_hist}")

# COMMAND ----------

# DBTITLE 1,Getting tables from catalog to process
def getTableListFromCatalog(catalog):
    # Lê as tabelas do catálogo informado

    if verbose:
        print(f"Reading catalog {catalog}")

    # Filtra as tabelas do DataFrame do sistema conforme os critérios definidos
    df = (
        df_system_table
        .where(f'table_catalog = "{catalog}" ')
        .where(f"last_altered > now() - interval {days_since_last_alt} days")
        .where(f"substr(table_name,1,2)!='__'")
    )

    # Coleta os resultados em uma lista de dicionários
    return_list = [
        data
        for data in df.select(
            col("table_catalog"), col("table_schema"), col("table_name"), col("last_altered"), col("table_type"),col("table_owner")
            ).collect()
    ]
    return return_list

# COMMAND ----------

# DBTITLE 1,Getting tables info
def getTableInfo(ptable):

    if verbose:
        print(f"{ptable['table_catalog']}.{ptable['table_schema']}.{ptable['table_name']} - Getting files info...")

    # TODO
    #try:
    #except

    dfReturn = (
        spark.sql(f"describe detail `{ptable['table_catalog']}`.`{ptable['table_schema']}`.`{ptable['table_name']}`")
        .select(
        lit(f"{batch_id}").alias("batchId"),
        lit(ptable['table_catalog']).alias("catalog"),
        lit(ptable['table_schema']).alias("schema"),
        lit(ptable['table_name']).alias("table"),
        lit(ptable['table_owner']).alias("table_owner"),
        lit(ptable['table_type']).alias("table_type"),
        lit("Y").alias("optimize"),
        "lastModified",
        "partitionColumns",
        "numFiles",
        "clusteringColumns",
        "clusterByAuto",
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

# DBTITLE 1,Main function
def processTable(table):

    fullname = f"`{table['table_catalog']}`.`{table['table_schema']}`.`{table['table_name']}`"

    if verbose:  
        print(f"{fullname}")

    try:

        dfDetail = getTableInfo (table)
        # dfDetail.persist(StorageLevel.MEMORY_ONLY)

        if verifyVacuum == "Y" or checkZorder == "Y" :

            if verbose: print(f"{fullname} - Getting history information...")

            try:

                dfHistory = spark.sql(f"desc history {fullname}") # .persist(StorageLevel.MEMORY_ONLY) # incompatible with serverless

                clean_fullname = 'history_'+fullname.replace(".", "_")

                dfHistory = dfHistory.withColumn("clean_fullname", lit(clean_fullname))

                if verbose: print(f"Writing cache of history at {table_file_stats_hist}...")

                dfHistory.write.mode("append").option("mergeSchema", "true").clusterBy("clean_fullname").saveAsTable(table_file_stats_hist)

                if verbose: print(f"Reading {table_file_stats_hist}...")
                dfHistory = spark.read.table(table_file_stats_hist).where(f"clean_fullname = '{clean_fullname}'")

                historyCount = dfHistory.count()
            except Exception as e:
                output = f"{e}"
                print(f"{fullname} - Error analyzing history for {fullname} {e}")
                historyCount = -1

            if verbose: print(f"{fullname} - Versions found : {historyCount}")

        if dfDetail.count() > 0:  ## there is at least one information regarding the table by describe detail

            ##  Cheking if last optimize was with zorder
            vacuum = "N/A"


            #Vaccum check and run if necessary

            if verifyVacuum == "Y":

                if verbose: print(f"{fullname} - Cheking Vaccum...")

                try:

                    v_last_altered = table['last_altered']
                    #v_last_altered = dfDetail.select("lastModified").collect()[0][0].strftime("%Y-%m-%d")

                    dfVacuum = (dfHistory
                                .where(f"timestamp < '{v_last_altered}' ")
                                .where("operation = 'VACUUM END'")
                                .where("operationParameters.status='COMPLETED'")
                                ) #.persist(StorageLevel.MEMORY_ONLY)


                    vacuum = "N"
                    vacuum_count = dfVacuum.count()

                    # There is not vacuum on table history or it was more the a week ago
                    if autoFixVacuum == "Y" and (vacuum_count == 0 or ( vacuum_count > 0 and '{v_last_altered}' < str_oneweekbehind )) :
                        vacuum = vacuum_table(fullname)


                except Exception as e:
                    output = f"{e}"
                    print(f"{fullname} - Error analyzing Vacuum for {fullname} {e}")


            ##  Cheking if last optimize was with zorder
            ##  It gets zorder information since the last optimize was done using zorder by
            ##  Only filter last 90 days
            zorder = 'N'
            zorderby =  ''
            lastOptimize = None
            optimize = 'N'

            if checkZorder == "Y":

                if verbose: print(f"{fullname} - Cheking zorder historic...")

                try:
                    zorderby,lastOptimize = (
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

                    if zorderby != "" : zorder="Y"

                    if AutoFixOptimize == "Y" and dfDetail.select("numFiles").collect()[0][0] > 1:
                        optimize = optimize_table(fullname,zorderby)

                except Exception as e:
                    output = f"{e}"
                    print(f"{fullname} - Error analyzing zorder {fullname} ")

            dfDetail = (
                        dfDetail
                        .withColumn("vacuum",lit(vacuum))
                        .withColumn("optimize",lit(optimize))
                        .withColumn("zorder",lit(zorder))
                        .withColumn("zorderby",lit(zorderby))
                        .withColumn("last_optimize",lit(lastOptimize))
                        )

            if verbose: print(f"{fullname} - Writing metadata...")

            dfDetail_write = (
                dfDetail
                .withColumn( "partitionColumns", concat_ws(",",col("partitionColumns")) )
                .withColumn( "timestamp" , current_timestamp())
            )


            dfDetail_write.write.mode("append").option("mergeSchema",True).format("delta").saveAsTable(table_file_stats) 
            # dfDetail.unpersist()

    except Exception as e:
        output = f"{e}"
        print(f"{fullname} - Error on {fullname} {e}")

    return list

# COMMAND ----------

# DBTITLE 1,Main function, where the files, vacuum and zorder are checked
# Note:  Be aware you are not zordering anything!
# # Parallelism when syncing the tables
import concurrent.futures, os
default_parallelism = 8 #os.cpu_count()

def processCatalog(catalog):


    tableList = getTableListFromCatalog(catalog)

    ## list will be used later
    # list = []

    # for table in tableList:
    #     processTable(table)

    with concurrent.futures.ThreadPoolExecutor(max_workers = default_parallelism) as executor:
        executor.map(processTable, tableList)

# COMMAND ----------

# DBTITLE 1,Optimize Function
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

# DBTITLE 1,Vacuum function
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



# COMMAND ----------

# DBTITLE 1,Optimizing stats function 
def autoClean():
    spark.sql(f"optimize {table_file_stats}")

    spark.sql(f"vacuum {table_file_stats}")
    spark.sql(f"vacuum {table_file_stats_hist}")

# COMMAND ----------

# DBTITLE 1,Calling the function listSmallfiles to append in list
if catalog == "*":  ## all catalogs will be processed

    catalogs = (
        spark.sql("SELECT catalog_name FROM system.information_schema.catalogs WHERE catalog_name <> 'system'")
        .collect()
    )

    str_catalogs = (', '.join([f"'{catalog['catalog_name']}'" for catalog in catalogs]))

    if verbose:
        print("Deleting some metadata")
    spark.sql(f"DELETE FROM {table_file_stats} WHERE catalog in ({str_catalogs}) and batchId = {batch_id}")

    for catalog_to_analyze in catalogs:

        if verbose:
            print(f"Analyzing catalog {catalog_to_analyze['catalog_name']}")

        tablesStats = processCatalog(catalog_to_analyze['catalog_name'])
        writeDataframe(catalog_to_analyze['catalog_name'],tablesStats)

else:

    spark.sql(f"DELETE FROM {table_file_stats} WHERE catalog = '{catalog}'")
    
    tablesStats = processCatalog(catalog)
    writeDataframe(catalog_to_analyze['catalog_name'],tablesStats)

autoClean()


# COMMAND ----------


