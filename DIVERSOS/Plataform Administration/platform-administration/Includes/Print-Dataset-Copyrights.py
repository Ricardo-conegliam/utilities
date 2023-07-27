# Databricks notebook source
# INCLUDE_HEADER_FALSE
# INCLUDE_FOOTER_FALSE

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

DA = DBAcademyHelper(**helper_arguments)
DA.init(install_datasets=True, create_db=False)

# COMMAND ----------

DA.print_copyrights()
