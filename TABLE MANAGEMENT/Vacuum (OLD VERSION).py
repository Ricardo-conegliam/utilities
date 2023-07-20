# Databricks notebook source
# MAGIC %python
# MAGIC #get list of all tables
# MAGIC
# MAGIC database_list = ["rededor","hotmart","take"]
# MAGIC
# MAGIC for database in database_list:
# MAGIC
# MAGIC     table_list = spark.sql(f"show tables from {database}").select('tableName').rdd.map(lambda x : x[0]).collect()
# MAGIC  
# MAGIC     for table in table_list:
# MAGIC         print("Running Vacuum on "+database+"."+table)
# MAGIC         try:
# MAGIC             spark.sql(f"VACUUM {database}.{table}")
# MAGIC         except:
# MAGIC             print(f"error on {database}.{table}")

# COMMAND ----------


