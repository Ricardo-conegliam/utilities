# Databricks notebook source
# MAGIC %sh
# MAGIC # install clojure
# MAGIC # NOTE: requires dbr with jdk11, set env in the cluster: JNAME=zulu11-ca-amd64
# MAGIC cd /tmp
# MAGIC curl -O https://download.clojure.org/install/linux-install-1.11.1.1149.sh
# MAGIC chmod +x linux-install-1.11.1.1149.sh
# MAGIC sudo ./linux-install-1.11.1.1149.sh

# COMMAND ----------

# MAGIC %sh
# MAGIC # get metabase and repos
# MAGIC rm -rf /local_disk0/metabase
# MAGIC mkdir /local_disk0/metabase
# MAGIC cd /local_disk0/metabase
# MAGIC
# MAGIC git clone https://github.com/metabase/metabase.git &&
# MAGIC git clone https://github.com/uucico/databricks-sql-driver.git

# COMMAND ----------

# MAGIC %sh
# MAGIC # workaround for metabase bug 
# MAGIC
# MAGIC cd /local_disk0/metabase/databricks-sql-driver
# MAGIC clojure -X:dev:build :project-dir "\"$(pwd)\""

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /local_disk0/metabase/databricks-sql-driver/target/databricks-sql.metabase-driver.jar
# MAGIC cp /local_disk0/metabase/databricks-sql-driver/target/databricks-sql.metabase-driver.jar /dbfs/FileStore
