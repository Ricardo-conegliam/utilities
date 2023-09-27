# Databricks notebook source
# MAGIC %md
# MAGIC # Local Kafka Deployment
# MAGIC
# MAGIC This notebook demonstrates how to install a local kafka service.
# MAGIC
# MAGIC To install Kafka, the Zookeeper service must also be installed.
# MAGIC
# MAGIC As a bonus, the configuration of Scram authentication was performed
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download and install Zookeeper

# COMMAND ----------

# MAGIC %sh
# MAGIC #installs and starts zookeeper
# MAGIC wget -qO- https://dlcdn.apache.org/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1-bin.tar.gz | tar xz -C /local_disk0
# MAGIC cp /local_disk0/apache-zookeeper-3.7.1-bin/conf/zoo_sample.cfg /local_disk0/apache-zookeeper-3.7.1-bin/conf/zoo.cfg 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Start Zookeeper

# COMMAND ----------

# MAGIC %sh
# MAGIC # start zookeeper
# MAGIC cd /local_disk0/apache-zookeeper-3.7.1-bin/
# MAGIC ./bin/zkServer.sh start

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download and Install Kafka

# COMMAND ----------

# MAGIC %sh
# MAGIC # get kafka distribution
# MAGIC wget -qO- https://dlcdn.apache.org/kafka/3.2.3/kafka_2.12-3.2.3.tgz | tar xz -C /local_disk0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Kafka Authentication

# COMMAND ----------

# MAGIC %sh
# MAGIC # sets up broker jaas
# MAGIC cat <<EOF>/local_disk0/kafka_2.12-3.2.3/config/kafka_server_jaas.conf
# MAGIC KafkaServer {
# MAGIC    org.apache.kafka.common.security.scram.ScramLoginModule required
# MAGIC    username="admin"
# MAGIC    password="admin-secret";
# MAGIC };
# MAGIC EOF
# MAGIC
# MAGIC # sets up server.properties for scram auth
# MAGIC cat <<EOF>>/local_disk0/kafka_2.12-3.2.3/config/server.properties
# MAGIC # List of enabled mechanisms, can be more than one
# MAGIC sasl.enabled.mechanisms=SCRAM-SHA-256
# MAGIC
# MAGIC # Specify one of of the SASL mechanisms
# MAGIC sasl.mechanism.inter.broker.protocol=SCRAM-SHA-256
# MAGIC security.inter.broker.protocol=SASL_PLAINTEXT
# MAGIC
# MAGIC # Without SSL encryption
# MAGIC listeners=PLAINTEXT://localhost:9092,SASL_PLAINTEXT://localhost:9093
# MAGIC advertised.listeners=PLAINTEXT://localhost:9092,SASL_PLAINTEXT://localhost:9093
# MAGIC
# MAGIC # jaas
# MAGIC listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin-secret";
# MAGIC EOF

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure credentials

# COMMAND ----------

# MAGIC %sh 
# MAGIC # create the initial credentials directly in zookeeper
# MAGIC (cd /local_disk0/kafka_2.12-3.2.3 &&
# MAGIC bin/kafka-configs.sh --zookeeper localhost:2181 --alter --add-config 'SCRAM-SHA-256=[iterations=8192,password=alice-secret],SCRAM-SHA-512=[password=alice-secret]' --entity-type users --entity-name alice)
# MAGIC
# MAGIC (cd /local_disk0/kafka_2.12-3.2.3 &&
# MAGIC bin/kafka-configs.sh --zookeeper localhost:2181 --alter --add-config 'SCRAM-SHA-256=[password=admin-secret],SCRAM-SHA-512=[password=admin-secret]' --entity-type users --entity-name admin)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Kafka

# COMMAND ----------

# MAGIC %sh
# MAGIC # start kafka
# MAGIC (cd /local_disk0/kafka_2.12-3.2.3 &&
# MAGIC KAFKA_OPTS=-Djava.security.auth.login.config=/local_disk0/kafka_2.12-3.2.3/config/kafka_server_jaas.conf ./bin/kafka-server-start.sh -daemon config/server.properties)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a topic

# COMMAND ----------

# MAGIC %sh
# MAGIC # create kafka topic, here we'll use the plaintext (no auth) listener
# MAGIC cd /local_disk0/kafka_2.12-3.2.3 &&
# MAGIC bin/kafka-topics.sh --create --topic teste --bootstrap-server localhost:9092

# COMMAND ----------

# MAGIC %md
# MAGIC ## Describe the topic

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /local_disk0/kafka_2.12-3.2.3 &&
# MAGIC bin/kafka-topics.sh --describe --topic teste --bootstrap-server localhost:9092

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate some random data and send to topic

# COMMAND ----------

# MAGIC %sh
# MAGIC # Generate some random data
# MAGIC (for i in {1..10}; do echo "Random data $RANDOM"; done) | 
# MAGIC (cd /local_disk0/kafka_2.12-3.2.3 && bin/kafka-console-producer.sh --topic teste --broker-list localhost:9092)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the topic

# COMMAND ----------

# use the sasl_plaintext listener (produce some messages using the console producer to test!)

df = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", "localhost:9093")
           .option("subscribe", "teste")
           .option("startingOffsets", "earliest")
           .option("kafka.security.protocol","SASL_PLAINTEXT")
           .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
           .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username=\"alice\" password=\"alice-secret\";")
           .load())

from pyspark.sql.functions import expr

display(df.withColumn("ValueAsString", expr("cast(value as STRING)")))
