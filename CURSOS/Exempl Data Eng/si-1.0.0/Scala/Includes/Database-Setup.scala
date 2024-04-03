// Databricks notebook source
println()
println()
println("Note: You can ignore the message 'Warning: classes defined within packages cannot be redefined without a cluster restart.'")

// COMMAND ----------

package com.databricks.training
object ClassroomHelper {
  def test_connection(url : String) : Unit = {
    val conn = java.sql.DriverManager.getConnection(url)
    try {
      val ps = conn.prepareStatement("SELECT 1")
      val rs = ps.executeQuery()
    } finally {
      conn.close()
    }
  }

  def sql_query(spark: org.apache.spark.sql.SparkSession, url : String, sql : String) : org.apache.spark.sql.DataFrame = {
    return spark.read.jdbc(url, s"($sql) query", new java.util.Properties)
  }

  def sql_update(url : String, sql : String, args : Any*) : Unit = {
    val conn = java.sql.DriverManager.getConnection(url)
    try {
      val ps = conn.prepareStatement(sql)
      args.zipWithIndex.foreach {case (arg, i) => {
        ps.setObject(i+1, arg)
      }}
      ps.executeUpdate()
    } finally {
      conn.close()
    }
  }

  def sql_update(url : String, sql : String, args : java.util.ArrayList[Object]) : Unit = {
    import scala.collection.JavaConverters._
    return sql_update(url, sql, args.asScala:_*)
  }

  def sql(url : String, sql : String) : Unit = {
    val conn = java.sql.DriverManager.getConnection(url)
    val stmt = conn.createStatement()
    val cmds = sql.split(";")
    var count = 0;
    try {
      for (cmd <- cmds) {
        if (!cmd.trim().isEmpty()) {
          stmt.addBatch(cmd)
          count += 1
        }
      }
      stmt.executeBatch()
    } finally {
      conn.close()
    }
  }
}

// COMMAND ----------

import com.databricks.training.ClassroomHelper
spark.conf.set(
  "fs.azure.account.key.spearfishtrainingstorage.blob.core.windows.net",
  "cJoNObBZt8C8xz2GwQmaHx25DmyRXAyd8TEp7/HDlT6jgt4+LeOjwYEhQ5SsCCrO0HRy6xlL8WZEM6xEwE9+9Q==")
val blobStoreBaseURL = "wasbs://training-container-clean@spearfishtrainingstorage.blob.core.windows.net/"
val username = com.databricks.logging.AttributionContext.current.tags(com.databricks.logging.BaseTagDefinitions.TAG_USER);
val userhome = s"dbfs:/user/$username/"
spark.conf.set("spark.databricks.delta.preview.enabled", true)
spark.conf.set("com.databricks.training.username", username)
spark.conf.set("com.databricks.training.userhome", userhome)
displayHTML("") //Supress Output

// COMMAND ----------

// MAGIC %python
// MAGIC class ClassroomHelper(object):
// MAGIC   scalaHelper=spark._jvm.__getattr__("com.databricks.training.ClassroomHelper$").__getattr__("MODULE$")
// MAGIC   @classmethod
// MAGIC   def test_connection(cls, url):
// MAGIC     cls.scalaHelper.test_connection(url)
// MAGIC   @classmethod
// MAGIC   def sql_query(cls, spark, url, sql):
// MAGIC     return spark.read.jdbc(url, "({}) query".format(sql))
// MAGIC   @classmethod
// MAGIC   def sql_update(cls, url, sql, *args):
// MAGIC     cls.scalaHelper.sql_update(url, sql, args)
// MAGIC   @classmethod
// MAGIC   def sql(cls, url, sql):
// MAGIC     cls.scalaHelper.sql(url, sql)
// MAGIC
// MAGIC blobStoreBaseURL = "wasbs://training-container-clean@spearfishtrainingstorage.blob.core.windows.net/"
// MAGIC username = spark.conf.get("com.databricks.training.username")
// MAGIC userhome = spark.conf.get("com.databricks.training.userhome")
// MAGIC None #Suppress output

// COMMAND ----------

println("Database-Setup successful.")
