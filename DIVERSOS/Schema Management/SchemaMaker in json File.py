# Databricks notebook source
from pyspark.sql.types import *
import json

# COMMAND ----------


schema = StructType([
      StructField("RecordNumber",IntegerType(),True),
      StructField("Zipcode",IntegerType(),True),
      StructField("ZipCodeType",StringType(),True),
      StructField("City",StringType(),True),
      StructField("State",StringType(),True),
      StructField("LocationType",StringType(),True)
  ])




# COMMAND ----------

schema_dir = "abfss://fe-shared-ssa-latam-unity-catalog@fesharedssalatamstorage1.dfs.core.windows.net/demos/cielo/schemas"

# COMMAND ----------

# dbutils.fs.mkdirs(schema_dir)

# COMMAND ----------

# dbutils.fs.ls("abfss://fe-shared-ssa-latam-unity-catalog@fesharedssalatamstorage1.dfs.core.windows.net/demos")

# COMMAND ----------

# display(dbutils.fs.ls("file:///tmp"))

# COMMAND ----------

with open("/tmp/customer.json","w") as f:
    json.dump(schema.json(),f)
    
dbutils.fs.cp("file:///tmp/customer.json",schema_dir)




# COMMAND ----------

dbutils.fs.head(f"{schema_dir}/customer.json")

# COMMAND ----------

dbutils.fs.ls(schema_dir)

# COMMAND ----------


with open('/tmp/customer.json', 'r') as f:
    schema_str = json.load(f)
    

schema_json = json.loads(schema_str)

schema = StructType.fromJson(schema_json)



# COMMAND ----------

schema2 = ( spark.read.json(f"{schema_dir}/customer.json")  )

# COMMAND ----------

display(schema2)

# COMMAND ----------


