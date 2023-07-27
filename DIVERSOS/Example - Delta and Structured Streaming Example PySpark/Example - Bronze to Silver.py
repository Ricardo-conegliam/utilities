# Databricks notebook source
# MAGIC %md
# MAGIC ### Exemplo - Bronze para Silver - Spark Structured Streaming com Tabelas Delta
# MAGIC
# MAGIC
# MAGIC - Utilizar DBR 10.4 ou superior
# MAGIC - Sugere-se rever: https://docs.databricks.com/structured-streaming/production.html

# COMMAND ----------

# DBTITLE 1,Widgets para configuração
# Definição de widgets default default para aplicação Delta em Spark SS
dbutils.widgets.text("source_table", "example.events_kafka_bronze", "Tabela de origem")
dbutils.widgets.text("destination_table", "example.events_kafka_silver", "Tabela de destino")
dbutils.widgets.text("checkpoint" ,"dbfs:/tmp/example/checkpoint_silver", "Local para o checkpoint") # em produção não utilizar dbfs! Usar um caminho do object storage. Não misturar checkpoints entre processos

# Configura variáveis para o processo
trigger              = {"processingTime": "30 seconds"}                 # ou "once=True" para transformar em batch. quanto maior o intervalo de processamento, mais eficiente a escrita na delta será.
checkpoint           = dbutils.widgets.get("checkpoint")
source_table         = dbutils.widgets.get("source_table")
destination_table    = dbutils.widgets.get("destination_table")

# COMMAND ----------

# DBTITLE 1,Preparação tabela silver
# MAGIC %sql CREATE SCHEMA IF NOT EXISTS example; -- em producao, especificar um location para um object storage - não usar DBFS para armazenamento de dados!
# MAGIC
# MAGIC DROP TABLE example.events_kafka_silver;
# MAGIC CREATE TABLE IF NOT EXISTS example.events_kafka_silver (
# MAGIC   key STRING,
# MAGIC   eventType STRING,
# MAGIC   messageIntegratedAt timestamp, -- estamos assumindo que este campo corresponde ao timestamp do evento, o convertemos de string para timestamp.
# MAGIC   accountType STRING,
# MAGIC   --
# MAGIC   p_eventDate date
# MAGIC ) PARTITIONED BY (p_eventDate) -- em producao, especificar um location para um object storage - não usar DBFS para armazenamento de dados!

# COMMAND ----------

# DBTITLE 1,Configurações para o streaming
# em caso de executar vários streamings no mesmo cluster, sugere-se configurar pools individuais para cada
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool-streaming-2")

# utilizar rocksdb como state store
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# para melhorar o condicionamento da tabela bronze, vamos utilizar o optimizedWrites e autoCompact - rever documentação https://docs.databricks.com/delta/optimizations/auto-optimize.html
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True) # este aumenta um pouco mais a latencia de escrita, mas garante arquivos maiores na saída

# evitar de apagar o checkpoint pois há perda de estado e a ingestao pode gerar dados duplicados!
#dbutils.fs.rm(checkpoint, True)

# COMMAND ----------

import json
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, DateType
from pyspark.sql.functions import to_date, col, from_json
from pyspark.sql import DataFrame

# carrega json com definicao do schema do evento
json_file = open('/dbfs/FileStore/json_event.json', 'r')
schemaFromJson = StructType.fromJson(json.loads(json_file.read()))
#print(schemaFromJson)

def parseIncomingEvent(df):
  return (
    df.select(from_json(col("value"), schemaFromJson).alias("parsed_json"))
      .selectExpr("parsed_json.eventType", "parsed_json.messageIntegratedAt", "parsed_json.payload.*") # pode ser comentada para obter uma estrutura nested com os campos dentro de uma unica coluna de nome payload
      .withColumnRenamed("messageIntegratedAt", "messageIntegratedAt_string")
      .withColumn("messageIntegratedAt", col("messageIntegratedAt_string").cast("timestamp"))
      .drop("messageIntegratedAt_string")
      .withColumn("p_eventDate", to_date(col("messageIntegratedAt")))    # adiciona coluna de particao
  )

# COMMAND ----------

# define objeto de streaming
stream = ( 
  spark.readStream
       .format("delta")
       .table(source_table)
       .transform(parseIncomingEvent)
       .writeStream
       .queryName("stream bronze table raw events to silver table") # pode ser útil na spark ui durante análises
       .trigger(**trigger)
       .option("checkpointLocation", checkpoint)
       .outputMode("append")
       .format("delta")
       .table(destination_table)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from example.events_kafka_silver
