// Databricks notebook source
dbutils.widgets.text("databaseName", "foo")
dbutils.widgets.text("startPart", "1")
dbutils.widgets.text("endPart", "2")

// COMMAND ----------

// get list of tables to count
val sourceDB = dbutils.widgets.get("databaseName")

//spark.sql("set spark.sql.shuffle.partitions = 8")

val listDB = spark.catalog.listTables(sourceDB).select("database", "name")

// COMMAND ----------

display(listDB)

// COMMAND ----------

display(
  spark.catalog.listColumns(sourceDB, "a")
)

// COMMAND ----------

// udf passthrough to table count
import org.apache.spark.sql.functions.udf

val countUDF = udf((db: String, tbl: String) => spark.table(db + "." + tbl).count())

// COMMAND ----------

// transform ist with udf for each table -- parallel section, inherits from previous rdd (see above)
import org.apache.spark.sql.functions.{col, lit}

val counts = listDB.withColumn("rowCount", lit(countUDF(col("database"), col("name"))))

//display(counts)

// COMMAND ----------

// output to delta table
spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
spark.sql("set spark.databricks.delta.autoOptimize.enabled = true")

counts.write.format("delta")
  .mode("overwrite")
  .save("dbfs:/tmp/ricardo.sugawara@databricks.com/table_cnt")

// COMMAND ----------

spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = false")
spark.sql("vacuum delta.`dbfs:/tmp/ricardo.sugawara@databricks.com/table_cnt` retain 0 hours")

// COMMAND ----------

dbutils.fs.ls("dbfs:/tmp/ricardo.sugawara@databricks.com/table_cnt")
