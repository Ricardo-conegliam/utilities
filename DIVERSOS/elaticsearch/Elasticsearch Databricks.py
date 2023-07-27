# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Elasticsearch
# MAGIC
# MAGIC <img src="https://static-www.elastic.co/v3/assets/bltefdd0b53724fa2ce/blt5ebe80fb665aef6b/5ea8c8f26b62d4563b6ecec2/brand-elasticsearch-220x130.svg" width="300">
# MAGIC
# MAGIC 1. Downloaded the ES-Spark jar for Spark 3 directly from https://search.maven.org/artifact/org.elasticsearch/elasticsearch-spark-30_2.12/7.15.2/jar to your local machine (e.g your laptop).
# MAGIC 2. In your Databricks workspace, go to one of your working folders, click "Create" -> "Library", and drag the jar from your local file system to the UI (where it says "Drop Jar here"). 
# MAGIC 3. Once the upload has finished, click "Create", and it will be added as a library to the folder you chose in the previous step.
# MAGIC 4. Launch a new cluster in your workspace.
# MAGIC 5. Once the new cluster is running, go to the "Libraries" tab of that cluster, and click "Install new" -> choose "Workspace" as the "Library Source" -> choose the jar from the location you previously placed it in -> click "Install".
# MAGIC 6. Once the installation has finished, attach this notebook to the cluster, and run write and/or read operations against your Elasticsearch cluster

# COMMAND ----------

# MAGIC %md
# MAGIC **Important**: In the following cells, replace `<ip-address>`, `<port>`, `<ssl>`, `<hostname>` and `<index>` with your Elasticsearch configuration

# COMMAND ----------

# DBTITLE 1,Test connectivity to your Elasticsearch cluster
# MAGIC %sh 
# MAGIC nc -vz ip-address port

# COMMAND ----------

# DBTITLE 1,Make trivial test dataframe
people = spark.createDataFrame( [ ("Bilbo",     50), 
                                  ("Gandalf", 1000), 
                                  ("Thorin",   195),  
                                  ("Balin",    178), 
                                  ("Kili",      77),
                                  ("Dwalin",   169), 
                                  ("Oin",      167), 
                                  ("Gloin",    158), 
                                  ("Fili",      82), 
                                  ("Bombur",  None)
                                ], 
                                ["name", "age"] 
                              )

# COMMAND ----------

# DBTITLE 1,Write to Elasticsearch
# Overwrite the data each time

# NOTE: We **must** set the es.nodes.wan.only property to 'true' so that the connector will connect to the node(s) specified by the `es.nodes` parameter.
#       Without this setting, the ES connector will try to discover ES nodes on the network using a broadcast ping, which won't work.
#       We want to connect to the node(s) specified in `es.nodes`.
( people.write
  .format( "org.elasticsearch.spark.sql" )
  .option( "es.nodes",   hostname )
  .option( "es.port",    port     )
  .option( "es.net.ssl", ssl      )
  .option( "es.nodes.wan.only", "true" )
  .mode( "overwrite" )
  .save( f"index/{index}" )
)

# COMMAND ----------

# DBTITLE 1,Read from Elasticsearch
# NOTE: We **must** set the es.nodes.wan.only property to 'true' so that the connector will connect to the node(s) specified by the `es.nodes` parameter.
#       Without this setting, the ES connector will try to discover ES nodes on the network using a broadcast ping, which won't work.
#       We want to connect to the node(s) specified in `es.nodes`.
df = (spark.read
      .format( "org.elasticsearch.spark.sql" )
      .option( "es.nodes",   hostname )
      .option( "es.port",    port     )
      .option( "es.net.ssl", ssl      )
      .option( "es.nodes.wan.only", "true" )
      .load( f"index/{index}" )
     )

display(df)
