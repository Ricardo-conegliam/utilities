-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC Rename table tests

-- COMMAND ----------

show create table cielo.dw.customer

-- COMMAND ----------

select * from cielo.dw.customer

-- COMMAND ----------

alter table cielo.dw.customer2 rename to cielo.dw.customer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Columns operations tests

-- COMMAND ----------



-- COMMAND ----------

alter table cielo.dw.customer add column (x int)

-- COMMAND ----------

alter table cielo.dw.customer alter column x first

-- COMMAND ----------

alter table cielo.dw.customer drop column x ;

-- COMMAND ----------

alter table cielo.dw.customer rename column x to y ;

-- COMMAND ----------

ALTER TABLE cielo.dw.customer SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '6')


-- COMMAND ----------

alter table cielo.dw.customer rename column x to y ;

-- COMMAND ----------

alter table cielo.dw.customer drop column y ;


-- COMMAND ----------

-- DBTITLE 1,Teste de SYNC de identity para backfill
create table cielo.dw.customersync as select * from cielo.dw.customer;

-- COMMAND ----------

show create table cielo.dw.customersync

-- COMMAND ----------

select count(*) from cielo.dw.customersync;

-- COMMAND ----------

alter table cielo.dw.customersync alter column id set default generated as identity

-- COMMAND ----------

SELECT to_timestamp('1970-01-01-15:20:10:987654', "yyyy-MM-dd-HH:mm:ss:SSSSSS");


-- COMMAND ----------


