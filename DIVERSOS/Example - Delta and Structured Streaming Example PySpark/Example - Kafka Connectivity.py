# Databricks notebook source
# MAGIC %md
# MAGIC ### Exemplo - Kafka para Bronze - Spark Structured Streaming e Delta
# MAGIC
# MAGIC
# MAGIC - Utilizar DBR 10.4 ou superior
# MAGIC - Sugere-se rever: https://docs.databricks.com/structured-streaming/production.html

# COMMAND ----------

# DBTITLE 1,Configuração do processo
# Definição de widgets default default para aplicação Kafka em Spark SS
dbutils.widgets.text("scope_secret", "ctakamiya_kafka", "Scope Secret")
dbutils.widgets.text("bootstrapServers", "pkc-41973.westus2.azure.confluent.cloud:9092", "Kafka Bootstrap Servers")
dbutils.widgets.text("clusterName", "test_ssa", "Nome do cluster Kafka")
dbutils.widgets.text("topicName" ,"json_events", "Nome do tópico Kafka")
dbutils.widgets.text("checkpoint" ,"dbfs:/tmp/example/checkpoint_bronze", "Local para o checkpoint") # em produção não utilizar dbfs! Usar um caminho do object storage. Não misturar checkpoints entre processos

# Configura variáveis para o processo
trigger              = {"processingTime": "30 seconds"}                 # ou "once=True" para transformar em batch. quanto maior o intervalo de processamento, mais eficiente a escrita na delta será.
checkpoint           = dbutils.widgets.get("checkpoint")
scope_secret         = dbutils.widgets.get("scope_secret")
bootstrapServers     = dbutils.widgets.get("bootstrapServers")
clusterName          = dbutils.widgets.get("clusterName")
topicName            = dbutils.widgets.get("topicName")
apiKey = dbutils.secrets.get(scope = scope_secret, key = "oci_username")       # ver https://docs.databricks.com/security/secrets/index.html
apiSecret = dbutils.secrets.get(scope = scope_secret, key = "oci_password") # usar databricks secrets para não expor senhas e tokens

# COMMAND ----------

# DBTITLE 1,Pipeline de streaming: source kafka, transformação, sink delta
# define objeto de streaming
stream = ( 
  spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", bootstrapServers)
       .option("kafka.security.protocol", "SASL_SSL")
       .option("kafka.sasl.jaas.config","kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(apiKey, apiSecret))
       .option("kafka.ssl.endpoint.identification.algorithm", "https")
       .option("kafka.sasl.mechanism", "PLAIN")
       .option("subscribe", topicName)
       .load()
       .limit(1)
)

display(stream)

# COMMAND ----------

# MAGIC %sh
# MAGIC nslookup qwxuticlhaya.streaming.sa-saopaulo-1.oci.oraclecloud.com
