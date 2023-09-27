# Databricks notebook source
table_name="main.schema01.TABLE1"
backup_table_name="main.schema01.TABLE1_BKP"

# COMMAND ----------

# DBTITLE 1,Creating test table
spark.sql(f"CREATE OR REPLACE TABLE {table_name} ( COD INTEGER,   DESCRIPTION STRING,  TIMESTAMP TIMESTAMP ) PARTITIONED BY (TIMESTAMP) ").display()

# COMMAND ----------

# DBTITLE 1,Inserting some data
spark.sql(f'INSERT INTO {table_name} VALUES ( 1 , "TESTE1" , "2023-02-01")')
spark.sql(f'INSERT INTO {table_name} VALUES ( 2 , "TESTE2" , "2023-02-02")')
spark.sql(f'INSERT INTO {table_name} VALUES ( 3 , "TESTE3" , "2023-02-03")')
spark.sql(f'INSERT INTO {table_name} VALUES ( 4 , "TESTE4" , "2023-02-04")')
spark.sql(f'INSERT INTO {table_name} VALUES ( 5 , "TESTE5" , "2023-02-05")')
spark.sql(f'INSERT INTO {table_name} VALUES ( 6 , "TESTE6" , "2023-02-06")')

# COMMAND ----------

# DBTITLE 1,Checking original table partition status
spark.sql(f"DESCRIBE DETAIL {table_name}").select("name","partitionColumns","numFiles","sizeInBytes").display()

# COMMAND ----------

# DBTITLE 1,Backing up the table using DEEP CLONE
spark.sql(f"CREATE TABLE {backup_table_name} DEEP CLONE {table_name};")

# COMMAND ----------

# DBTITLE 1,Checking backup table
spark.sql(f"DESCRIBE DETAIL {backup_table_name}").select("name","partitionColumns","numFiles","sizeInBytes").display()

# COMMAND ----------

# DBTITLE 1,Saving table with no partitions
df_unpartitioned = spark.sql(f'SELECT * FROM {table_name} WHERE 1=1')
df_unpartitioned.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Checking table without partitions
spark.sql(f"DESCRIBE DETAIL {table_name}").select("name","partitionColumns","numFiles","sizeInBytes").display()

# COMMAND ----------

# DBTITLE 1,Cheking the data
spark.sql(f"SELECT * FROM {table_name}").display()

# COMMAND ----------

# DBTITLE 1,Optimizing
spark.sql(f"OPTIMIZE {table_name}").display()

# COMMAND ----------

# DBTITLE 1,Vacuuning
spark.sql(f"VACUUM {table_name}").display()

# COMMAND ----------

# DBTITLE 1,Droping BACKUP table
spark.sql(f"DROP TABLE {backup_table_name}").display()

# COMMAND ----------


