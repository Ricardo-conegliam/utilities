# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Author :  Ricardo Conegliam
# MAGIC
# MAGIC Objective :  This notebook have some auxiliar management of tables
# MAGIC

# COMMAND ----------

from datetime import datetime
from pyspark.sql.types import IntegerType,LongType
from pyspark.sql.functions import col
from delta import DeltaTable

# COMMAND ----------

## This function will create table backups (_bkp_{timestamp}) for all tables in given catalog and schema

def backupTables(catalog,schema,table):

    currentTime = datetime.now().strftime('%Y%m%d%H%M%S')

    print(f"Backup {catalog}.{schema}.{table} to {catalog}.{schema}.{table}_bkp_{currentTime}...")

    # spark.sql(f"CREATE TABLE {catalog}.{schema}.{table}_bkp_{currentTime} AS SELECT * FROM {catalog}.{schema}.{table}")

    df = DeltaTable.forName(spark,f"{catalog}.{schema}.{table}")
    df.clone(f"{catalog}.{schema}.{table}_bkp_{currentTime}", isShallow=False)


# COMMAND ----------


# This function will change int to bigint for all tables witch is not _bkp_ neither _new for given catalog and schema

def intToBigint(catalog,schema):

    # it will ignore tables with _bkp_ in the name!

    tableList = (spark.sql
                 (f"show tables from {catalog}.{schema}")
                 .select("tableName")
                 .where("tableName not like '%_bkp_%' and tableName not like '%_new'")
                 .rdd.map(lambda x : x[0])
                 .collect()
    )

    for table in tableList:

        print(f"Analyzing columns on {catalog}.{schema}.{table}...")

        # tab = DeltaTable.forName(spark,f"{catalog}.{schema}.{table}")
        df = spark.read.table(f"{catalog}.{schema}.{table}")

        tableHasModification = False
        

        for column in df.dtypes:

            if column[1] == "int":
                print(f"Changing INT column {column[0]}:{column[1]} on {catalog}.{schema}.{table}...")
                df = df.withColumn(column[0],col(column[0]).cast(LongType()))
                tableHasModification = True
            
        if tableHasModification:
            backupTables(catalog,schema,table)
            
            print(f"Writing changed table {catalog}.{schema}.{table}_new...")
            df.write.saveAsTable(f"{catalog}.{schema}.{table}_new")



# COMMAND ----------

# This function will get _new table and create a new one with the Table Name without _new

def replaceByNewTables(catalog,schema):
# it will rname *_new tables to its original name

    tableList = (spark.sql
                 (f"show tables from {catalog}.{schema}")
                 .select("tableName")
                 .where("tableName like '%_new'")
                 .rdd.map(lambda x : x[0])
                 .collect()
    )

    for table in tableList:

        finalTableName = table.replace("_new",'')

        # tab = DeltaTable.forName(spark,f"{catalog}.{schema}.{table}")
        df = spark.read.table(f"{catalog}.{schema}.{table}")
        
        print(f"Dropping {catalog}.{schema}.{finalTableName}...")

        spark.sql(f"DROP TABLE {catalog}.{schema}.{finalTableName}")
           
        print(f"Writing table {catalog}.{schema}.{finalTableName}...")
        df.write.saveAsTable(f"{catalog}.{schema}.{finalTableName}")
        
        print(f"Dropping {catalog}.{schema}.{table}...")        
        spark.sql(f"DROP TABLE {catalog}.{schema}.{table}")



# COMMAND ----------

#This function will drop all tables with mask (for like in where clouse) in given catalog and schema

def dropTables(catalog,schema,mask):

    tableList = (spark.sql
                 (f"show tables from {catalog}.{schema}")
                 .select("tableName")
                 .where(f"tableName like '{mask}'")
                 .rdd.map(lambda x : x[0])
                 .collect()
    )

    for table in tableList:
        print(f"Dropping {catalog}.{schema}.{table}...")
        spark.sql(f"DROP TABLE {catalog}.{schema}.{table}")

# COMMAND ----------

catalog = "main"
schema = "schema01"

## THOSE PROCEDURES DO NOT WORK FOR GENERATED BY DEFAULT AS IDENTITY


# this funcion will generate _new tables with the bigint columns
intToBigint(catalog,schema)

# this function will get the _new tables and write to the original name 
replaceByNewTables(catalog,schema)

#ONLY IF EVERTHING IS OK, then you can drop your bkp tables
# 
dropTables(catalog,schema,"%_bkp_%")




# COMMAND ----------

# %sql
# drop table if exists main.schema01.x;
# drop table if exists main.schema01.x_new;
# create table if not exists main.schema01.x  (a int,b int,z int);


# COMMAND ----------

# dropTables(catalog,schema,"%_bkp_%")
# dropTables(catalog,schema,"%new")

# COMMAND ----------


