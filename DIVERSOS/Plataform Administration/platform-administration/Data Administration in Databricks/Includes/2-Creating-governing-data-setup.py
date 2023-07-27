# Databricks notebook source
# INCLUDE_HEADER_FALSE
# INCLUDE_FOOTER_FALSE

# COMMAND ----------

import re

class DBAcademyHelper():
     def __init__(self):
        import re
        
        # Do not modify this pattern without also updating the Reset notebook.
        username = spark.sql("SELECT current_user()").first()[0]
        clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
        self.catalog = f"dbacademy_{clean_username}"        
        spark.conf.set("da.catalog", self.catalog)

da = DBAcademyHelper()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${da.catalog}' AS `Catalog`
