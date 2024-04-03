# Databricks notebook source
#get list of all tables

database_list = ["rededor","hotmart","take"]

for database in database_list:

    table_list = spark.sql(f"show tables from {database}").select('tableName').rdd.map(lambda x : x[0]).collect()
 
    for table in table_list:
        print("Running Vacuum on "+database+"."+table)
        try:
            spark.sql(f"VACUUM {database}.{table}")
        except:
            print(f"error on {database}.{table}")

# COMMAND ----------


