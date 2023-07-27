// Databricks notebook source
dbutils.fs.put("/users/tmp/test.json", """
{"string":"string1","int":1,"array":[1,2,3],"dict": {"key": "value1"}}
{"string":"string2","int":2,"array":[2,4,6],"dict": {"key": "value2"}}
{"string":"string3","int":3,"array":[3,6,9],"dict": {"key": "value3", "extra_key": "extra_value3"}}
""", true)

// COMMAND ----------

// MAGIC %scala
// MAGIC
// MAGIC val testJsonData = spark.read.option("multiline", "false").json("/users/tmp/test.json")
// MAGIC
// MAGIC display(testJsonData)

// COMMAND ----------

// MAGIC %r 
// MAGIC library(SparkR)
// MAGIC
// MAGIC testJsonData <- read.df("/users/tmp/test.json", "json")
// MAGIC
// MAGIC display(testJsonData)

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC testJsonData = spark.read.json("/users/tmp/test.json")
// MAGIC
// MAGIC display(testJsonData)

// COMMAND ----------

// MAGIC %sql 
// MAGIC CREATE TEMPORARY VIEW jsonTable
// MAGIC USING json
// MAGIC OPTIONS (path="/users/tmp/test.json")

// COMMAND ----------

// MAGIC %sql SELECT * FROM jsonTable

// COMMAND ----------

dbutils.fs.put("/users/tmp/multi-line.json", """[
    {"string":"string1","int":1,"array":[1,2,3],"dict": {"key": "value1"}},
    {"string":"string2","int":2,"array":[2,4,6],"dict": {"key": "value2"}},
    {
        "string": "string3",
        "int": 3,
        "array": [
            3,
            6,
            9
        ],
        "dict": {
            "key": "value3",
            "extra_key": "extra_value3"
        }
    }
]""", true)

// COMMAND ----------

val mldf = spark.read.option("multiline", "true").json("/users/tmp/multi-line.json")
mldf.show(false)

// COMMAND ----------

// MAGIC %sql 
// MAGIC CREATE TEMPORARY VIEW multiLineJsonTable
// MAGIC USING json
// MAGIC OPTIONS (path="/users/tmp/multi-line.json",multiline=true)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from multiLineJsonTable
