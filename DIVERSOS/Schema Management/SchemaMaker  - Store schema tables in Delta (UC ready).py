# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Author :  Ricardo Conegliam
# MAGIC
# MAGIC Date : Jun, 13th 2003
# MAGIC
# MAGIC Objective : This notebook is a demo for store your schema on a Delta Table insted of inside your code ou json files on some directory.
# MAGIC
# MAGIC How to retrieve the schema:
# MAGIC
# MAGIC import pyspark.sql.types as T
# MAGIC
# MAGIC schema_str = (spark.read.table(f"{schemasTable}")
# MAGIC               .where(f'active = "Y" and catalog = "{catalog}" and database = "{schema}" and tableName = "{tableName}"')
# MAGIC               .first()['tableSchema']
# MAGIC )
# MAGIC
# MAGIC tableSchema = T._parse_datatype_string(schema_str)

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.types import *


# COMMAND ----------

# DBTITLE 1,Change only this cell!
schemasTable = "main.default.tableSchemas"

catalog = "cielo"
schema = "DW"
tableName = "organizations"

tableSchema = StructType([
      StructField("Index",IntegerType(),True),
      StructField("OrganizationId",StringType(),True),
      StructField("Name",StringType(),True),
      StructField("Website",StringType(),True),
      StructField("Country",StringType(),True),
      StructField("Description",StringType(),True),
      StructField("Founded",StringType(),True),
      StructField("Industry",StringType(),True),
      StructField("Numberofemployees",StringType(),True)
  ])



# COMMAND ----------

# DBTITLE 1,Creating table for schemas store
spark.sql(f"create table if not exists {schemasTable} (\
          catalog string, \
              database string, \
              tableName string, \
              tableSchema String, \
              createdAt timestamp, \
              lastUpdate timestamp, \
              active string \
              ) \
        ")

spark.sql(f'update {schemasTable} \
            set active = "N", lastUpdate = current_timestamp() \
            where active = "Y" and catalog = "{catalog}" and database = "{schema}" and tableName = "{tableName}"')


# COMMAND ----------

# DBTITLE 1,Generating schema string to store at the schemas table
dfSchema = spark.createDataFrame([],schema=tableSchema)
str_schema = dfSchema.schema.simpleString()

spark.sql(f'insert into {schemasTable} \
          values ("{catalog}","{schema}","{tableName}","{str_schema}",current_timestamp(),current_timestamp(),"Y")')

display(spark.sql(f'select * from {schemasTable} where active = "Y"'))

# COMMAND ----------

# DBTITLE 1,Testing the schema
import pyspark.sql.types as T


schema_str = (spark.read.table(f"{schemasTable}")
              .where(f'active = "Y" and catalog = "{catalog}" and database = "{schema}" and tableName = "{tableName}"')
              .first()['tableSchema']
)

varTableSchema = T._parse_datatype_string(schema_str)

# you can create a dataset or use as spark.read.schema(tableSchema).load ....

dfschemasTable = spark.createDataFrame([],schema=varTableSchema)

display(dfschemasTable.schema)

# COMMAND ----------

spark.sql(f"OPTIMIZE {schemasTable}")

# COMMAND ----------


