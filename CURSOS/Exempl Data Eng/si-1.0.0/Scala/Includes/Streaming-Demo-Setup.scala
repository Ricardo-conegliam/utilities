// Databricks notebook source
// MAGIC
// MAGIC %run ./Classroom-Setup

// COMMAND ----------


dbutils.fs.rm(userhome + "/streaming-demo", true)

spark.read
  .json("/mnt/training/definitive-guide/data/activity-data-with-geo.json/")
  .toJSON
  .withColumnRenamed("value", "body")
  .write
  .mode("overwrite")
  .format("delta")
  .save(userhome + "/streaming-demo")

val activityStreamDF = (spark.readStream
  .format("delta")
  .option("maxFilesPerTrigger", 1)
  .load(userhome + "/streaming-demo")
)
