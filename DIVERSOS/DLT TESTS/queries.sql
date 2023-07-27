-- Databricks notebook source
select count(*) from table1_ext

-- COMMAND ----------

select purchase_id,count(*) from  table1_ext group by purchase_id order by 1 desc

-- COMMAND ----------

create table table1_new deep clone table1_ext

-- COMMAND ----------

select count(*) from table1_ext

-- COMMAND ----------

describe extended dltschema1.table1_new

-- COMMAND ----------

create table table1_ext location "dbfs:/users/dlttest/table1_ext" as select * from dltschema1.table1_new

-- COMMAND ----------

describe extended table1_ext

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.head("dbfs:/users/dlttest/checkpoint/sources/0/0")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.head("dbfs:/users/dlttest/checkpoint/offsets/1")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/users/dlttest/checkpoint",True)
-- MAGIC
-- MAGIC

-- COMMAND ----------

select count(*) from DLTschema1.table1

-- COMMAND ----------

select purchase_id,count(*) from  DLTschema1.table1 group by purchase_id order by 1 desc

-- COMMAND ----------

drop table DLTschema1.table1

-- COMMAND ----------


