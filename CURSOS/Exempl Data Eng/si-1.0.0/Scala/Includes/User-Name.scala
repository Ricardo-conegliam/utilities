// Databricks notebook source
{val tags = com.databricks.logging.AttributionContext.current.tags

//*******************************************
// GET USERNAME AND USERHOME
//*******************************************

// Get the user's name
val username = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
val userhome = s"dbfs:/user/$username"

// Set the user's name and home directory
spark.conf.set("com.databricks.training.username", username)
spark.conf.set("com.databricks.training.userhome", userhome)}

print("Success!\n")

val username = spark.conf.get("com.databricks.training.username")
val userhome = spark.conf.get("com.databricks.training.userhome")

// COMMAND ----------

// MAGIC %python
// MAGIC username = spark.conf.get("com.databricks.training.username")
// MAGIC userhome = spark.conf.get("com.databricks.training.userhome")
// MAGIC
// MAGIC None # suppress output
// MAGIC
