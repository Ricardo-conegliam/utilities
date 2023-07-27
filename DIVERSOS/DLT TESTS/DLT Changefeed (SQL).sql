-- Databricks notebook source
CREATE LIVE TABLE table1_sql
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS
Select * from hotmart.source1;


CREATE LIVE TABLE feed1_sql
AS 
SELECT *
FROM table_changes('hotmart.table1_sql');


