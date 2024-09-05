// Databricks notebook source
import org.apache.spark.sql.functions._

// COMMAND ----------

val checkPointMigration = spark.sql("""
  select distinct erp, cdcTopic, checkPointFolder,
  concat(checkPointFolder, "/bronze/checkpoint_", erp, "/cp001") as oldPath,
  concat(checkPointFolder, "/bronze_spark3x/checkpoint_", erp, "_", cdcTopic, "/cp001") as newPath
  from dev_cdl._config
""")

// checkPointMigration.write.format("delta").saveAsTable("checkpoint_migration_check")   - saved once for checkpoint dir check in kafka_to_bronze in dev
display(checkPointMigration)

// COMMAND ----------

checkPointMigration.collect().foreach { r => 
  val oldPath = r.getAs[String]("oldPath")
  val newPath = r.getAs[String]("newPath")
  println(s"${oldPath}\n  => ${newPath}")
  
  val cpChkpt = dbutils.fs.cp(oldPath, newPath, true)
//   if (cpChkpt) dbutils.fs.rm(oldPath, true)    // schedule removal after validation and production migration
}
