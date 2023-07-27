# Databricks notebook source
# MAGIC %md
# MAGIC ### DLT - Kafka to Bronze
# MAGIC

# COMMAND ----------

scope_secret = "rconegliam_kafka_eh"
bootstrapServers = "fe-shared-ssa-latam-eventhubs.servicebus.windows.net:9093"
clusterName = "fe-shared-ssa-latam-eventhubs"

apiUsername = "$ConnectionString"
apiPassword = dbutils.secrets.get(scope=scope_secret, key="apiPassword")

sourceSignature = "source1"
nameSpace = "NameSpace1"
eventhubsName = "eventHub1"

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    expr,
    col,
    current_timestamp,
    from_json,
)
from pyspark.sql import DataFrame

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
)

schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("description", StringType(), True)
    ]
)



# COMMAND ----------

def createDLTtable(eventName,tableName):

    @dlt.table(
        name=tableName,
        comment="DLT backfill test",
        table_properties={
            "quality": "bronze",
            "pipelines.reset.allowed" : "false",
        },
        path = "dbfs:/users/dlttest/table1",
    )
    @dlt.expect("Valid purchase_id", "id IS NOT NULL")
    @dlt.expect("Description is not Uppercase","isUppercase(description)")
    def createTable():
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrapServers)
            .option("kafka.security.protocol", "SASL_SSL")
            .option(
                "kafka.sasl.jaas.config",
                "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(
                    apiUsername, apiPassword
                ),
            )
            .option("kafka.ssl.endpoint.identification.algorithm", "https")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("subscribe", eventName)
            .option("startingOffsets","""{"topic1":{"0":80}}""")
            .option("failOnDataLoss", "true")
            .load()
            .select(
                from_json(col("value").cast("string"), schema).alias("json")
            )
            .selectExpr("json.*")
            .withColumn("bronze_timestamp", current_timestamp())
        )




# COMMAND ----------

createDLTtable("topic1","table1")
