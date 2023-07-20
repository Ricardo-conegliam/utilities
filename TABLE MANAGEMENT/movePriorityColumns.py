# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC YOU CAN DO THE FIRST COLUMNS USING ALTER TABLE ALTER COLUMN X FIRST !!!!!!!!!!!
# MAGIC
# MAGIC THIS IS ONLY AND THE CUSTOMER DELTA VERSION CANNOT BE UPGRADED 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Author :  Ricardo Conegliam
# MAGIC
# MAGIC Objective :  This notebook have some auxiliar management of tables, like backup, reorg, vacuum and others
# MAGIC

# COMMAND ----------

from datetime import datetime
from delta import DeltaTable



# COMMAND ----------

def backupTables(catalog,schema,table):

    currentTime = datetime.now().strftime('%Y%m%d%H%M%S')

    print(f"Backup {catalog}.{schema}.{table} to {catalog}.{schema}.{table}_bkp_{currentTime}...")

    # spark.sql(f"CREATE TABLE {catalog}.{schema}.{table}_bkp_{currentTime} AS SELECT * FROM {catalog}.{schema}.{table}")

    df = DeltaTable.forName(spark,f"{catalog}.{schema}.{table}")
    df.clone(f"{catalog}.{schema}.{table}_bkp_{currentTime}", isShallow=False)

    return f"{catalog}.{schema}.{table}"

# COMMAND ----------

def movePriorityColumns(catalog,schema,table,firstColumns):
    # Parameter is a python list of the columns you want to be first on the table

    bkpTable = backupTables(catalog,schema,table)

    df = spark.read.table(bkpTable)

    others = []

    #just checking if the firstcolumns are actually on the table
    for colcheck in firstColumns:
        if not colcheck in df.columns:
            print (f"Column {colcheck} not on the table!!!")
            raise "Column doesnt exist on table"

    for c in df.columns:
        if not c in firstColumns:
            others.append(c)
    

    df = df.select( *firstColumns , *others)

    print(f"Dropping {catalog}.{schema}.{table}...")

    spark.sql(f"DROP TABLE {catalog}.{schema}.{table}")

    print(f"Writing reorganized columns table {catalog}.{schema}.{table}...")

    df.write.saveAsTable(f"{catalog}.{schema}.{table}")

    return df



# COMMAND ----------

# DBTITLE 1,Reorganizing the columns of your table
catalog = "main"
schema = "schema01"

movePriorityColumns(catalog,schema,"events",["value","customer"])

# COMMAND ----------


