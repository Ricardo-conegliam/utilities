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
apiKey = dbutils.secrets.get(scope = scope_secret, key = "key")       # ver https://docs.databricks.com/security/secrets/index.html
apiSecret = dbutils.secrets.get(scope = scope_secret, key = "secret") # usar databricks secrets para não expor senhas e tokens

# COMMAND ----------

# DBTITLE 1,Preparação schema e tabela bronze
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS example; -- em producao, especificar um location para um object storage - não usar DBFS para armazenamento de dados!
# MAGIC
# MAGIC -- DROP TABLE example.events_kafka_bronze;
# MAGIC CREATE TABLE IF NOT EXISTS example.events_kafka_bronze (
# MAGIC   key string,   -- trocamos de binary para string
# MAGIC   value binary, -- revisar
# MAGIC   topic string,
# MAGIC   partition int,
# MAGIC   offset long,
# MAGIC   timestamp timestamp,
# MAGIC   timestampType int,
# MAGIC   p_ingestionDate date
# MAGIC ) PARTITIONED BY (
# MAGIC   p_ingestionDate
# MAGIC ) -- em producao, especificar um location para um object storage - não usar DBFS para armazenamento de dados!

# COMMAND ----------

# DBTITLE 1,Configurações para o streaming
# em caso de executar vários streamings no mesmo cluster, sugere-se configurar pools individuais para cada
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool-streaming-1")

# utilizar rocksdb como state store
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# para melhorar o condicionamento da tabela bronze, vamos utilizar o optimizedWrites e autoCompact - rever documentação https://docs.databricks.com/delta/optimizations/auto-optimize.html
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True) # este aumenta um pouco mais a latencia de escrita, mas garante arquivos maiores na saída

# evitar de apagar o checkpoint pois há perda de estado e a ingestao pode gerar dados duplicados!
#dbutils.fs.rm(checkpoint, True)

# COMMAND ----------

# DBTITLE 1,Função de transformação do evento na bronze
from pyspark.sql.functions import to_date, col
from pyspark.sql import DataFrame

# not required for avro
def processIncomingEvent(df):
  columns_to_cast = ["key", "value"] # quais colunas queremos trocar de binary para string
  
  return (
    df.select(
      *(c for c in df.columns if c not in columns_to_cast),        # mantem as demais as-is
      *(col(c).cast("string").alias(c) for c in columns_to_cast)   # converte colunas na lista
    )
      .withColumn("p_ingestionDate", to_date(col("timestamp")))    # adiciona coluna de particao
  )

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
       #.transform(processIncomingEvent)
       .writeStream
       .queryName("stream kafka events to bronze table") # pode ser útil na spark ui durante análises
       .trigger(**trigger)
       .option("checkpointLocation", checkpoint)
       .outputMode("append")
       .format("delta")
       .table("example.events_kafka_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from example.events_kafka_bronze
