# Databricks notebook source
# MAGIC %sh
# MAGIC wget https://dlcdn.apache.org/jmeter/binaries/apache-jmeter-5.5.tgz
# MAGIC tar -zxvf apache-jmeter-5.5.tgz

# COMMAND ----------

# MAGIC %sh
# MAGIC ip addr list

# COMMAND ----------

# MAGIC %sh
# MAGIC cd apache-jmeter-5.5
# MAGIC echo server.rmi.ssl.disable=true >> bin/jmeter.properties
# MAGIC bin/jmeter-server -Djava.rmi.server.hostname=172.16.8.132

# COMMAND ----------

# MAGIC %sh
# MAGIC ps -auxwww | grep jmeter
