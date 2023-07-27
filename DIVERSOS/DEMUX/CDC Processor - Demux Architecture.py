# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Processor
# MAGIC ## Multiplexing/Demultiplexing Architecture
# MAGIC
# MAGIC This notebook implement CDC Processor with streaming approach showcasing the Multiplex/Demultiplex Architecture.
# MAGIC
# MAGIC ### Description
# MAGIC This architecture main focus is to reduce the number of streams running when ingesting data. The pattern involves copying multiple tables/sources worth of data into one Bronze Delta table, and then breaking that data out into separate tables during the *Bronze->Silver* step of the pipeline.  The breaking out the data into separate tables is what is called Demultiplexing or “demux”.
# MAGIC
# MAGIC With the simplest form of this pattern, there is a stream pulling from the streaming source and writing all the data to one Bronze Delta table, and then one or more streams pulling from the Bronze Delta table into multiple Silver Delta tables.  With this pattern you get a historical copy of data with very little complexity and then you can do the more complex processing in the next layer.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Multiplexing
# MAGIC
# MAGIC How the columns are defined in the Bronze table are what makes the Streaming Demux pattern work.  Two of the columns need to be the columns that are being partitioned on (example - table name, date).  If there are other columns that will be common across all the data being stored in this Bronze table then they can be defined as part of the table schema as well.
# MAGIC
# MAGIC One of the columns needs to be the `body` which will hold the payload for the record. The columns can be `binary`, `avro`, `json` or `string`
# MAGIC
# MAGIC An example schema can be:
# MAGIC
# MAGIC | Column Name     | Column Type |
# MAGIC |-----------------|-------------|
# MAGIC | source_id       | string      |
# MAGIC | table_name      | string      |
# MAGIC | schema_version  | string      |
# MAGIC | processing_time | timestamp   |
# MAGIC | processing_date | date        |
# MAGIC | filename        | string      |
# MAGIC | body            | string      |
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

############################
# Function used to multiplex source data into a single bronze table. 
#
# By passing a single parent table, Spark is able to recursively load all files in all directories in a single dataframe.
# Spark will also add a column per directory value if it detects a partitioning patterns (ex: "/layer=landing/source=mars") will result in a two added columns (`layer` and `source`) with all values seen during read.

# If there are a mix of different file types, you can either filter based on the filename extension for the right files or explicitly load the paths where the files are at directly
# ex: Spark.read.format("text").load(path+"/*/*/*.csv")
# This approach will not add columns per directory value if it detects a partitioning patterns. 
# In this scenario, you will need to explicitly parse and extract the required values from the file path (can be retrieved by `input_file_name`)

# The table will be partitioned on `source_id`, `table_name` and `processing_date`. 
# This will enable partition prunning when filtering the batch later on (i.e. the dataframe will not need to be fully scanned to filter for the tables).
# Adding `processing_date` as a partition column will help in applying retention. As dropping older partitions should not affect and currently running operations.
############################# 

from pyspark.sql.functions import input_file_name, current_timestamp, col

def multiplex_to_bronze(data_folder, checkpoint_folder, file_format):
  full_table_name = "cz_bronze.demux"

  bronze_df = (
    spark
      .readStream
      .format("text")
      .load(data_folder)
      .withColumn("filename", input_file_name())
      .withColumn("processing_time", current_timestamp())
      .withColumn("processing_date", col("processing_time").cast("date"))
      .filter(f"endswith(filename, '.{file_format}')") # This may or may not be needed depending on how you organize your landing data
      .selectExpr("source as source_id", "collection as table_name", "'1' as schema_version ", "processing_time", "processing_date", "filename", "value as body")    
  )

  checkpoint_path = checkpoint_folder+"/bronze/demux"
  query_name = "bronze-demux-cdc"

  (
    bronze_df
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpoint_path)
      .queryName(query_name)
      .partitionBy("source_id", "table_name", "processing_date")
      .trigger(availableNow=True)
      .table(full_table_name)
  )


# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Demultiplexing
# MAGIC
# MAGIC Once the Bronze table is in place, the actual demux part of the pattern can be applied. The implementation will allow you to specify multiple tables within one stream and multiple streams depending on the table names passed in. It also dynamically loads a schema and key columns for a given table. This means you can have a single generic function that will be able to handle any table in the batch.
# MAGIC
# MAGIC ## Overall Logic Flow
# MAGIC The example Demux executes the following logic:
# MAGIC
# MAGIC - It reads in the set of groups of table names that was passed to it.
# MAGIC - It loops through the table name groups.  For each table name group:
# MAGIC   - It does a readStream from the Bronze table and filters on the list of table names in the group
# MAGIC   - It creates an instance of a `demux_to_silver` function with a closure of the list tables to be processed so that the `foreachBatch` function will have access to it
# MAGIC   - It does a writeStream and calls the `foreachBatch` function with the `demux_to_silver` instance created.  To ensure each stream has a unique query name and checkpoint file, it incorporates the name of the table group into both
# MAGIC - Inside the `demux_to_silver` foreachBatch function:
# MAGIC   - It gets the list of table names in the microbatch from the closure passed
# MAGIC   - It loops through each table name, and 
# MAGIC     - Filters the batch for the rows matching this table name and divides the batch to be processed per schema version
# MAGIC     - Pulls the schema and parses the payload data
# MAGIC     - Creates the table if not exists
# MAGIC     - Evolves the schema if needed
# MAGIC     - Merges into the Silver table
# MAGIC
# MAGIC The logic flow is fairly simple, but there are details that have to be considered in order for the above logic to function properly.  Those are discussed below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Management
# MAGIC
# MAGIC When implementing a Demux, there needs to be a mechanism to manage the schemas for all of the data in the Bronze table. Since the payload data is stored as a binary or a string, it will need to be parsed with a schema.  To avoid hard-coding a schema in the code for each Silver table, the schemas should be managed outside of the pipeline. A schema registry, such as one that comes with Confluent Kafka, is a common way to do this.  With Confluent Kafka when records are written the schema is recorded as part of the write. Those schemas can be pulled dynamically during the Demux processing by using a schema registry client.
# MAGIC
# MAGIC Another approach is to store the schema as a table where the  schema for each table in the batch is stored as a column in a Delta table called `schemas`. The `foreachBatch` function uses the name of the table it is currently processing to dynamically query the schemas table so that it can get the schema to parse the data.  Since the schemas are in a Delta table the data is cached, pulling schemas multiple times during the processing of each microbatch doesn’t add any significant latency. Caching the `schemas` table in memory is also a good approach. The data needs to be populated in the `schemas` table ahead of time to make this work.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Evolution

# COMMAND ----------

############################
# Utility functions that supports schema evolution
#
# Schema evolution can be acheived by comparing the schema of a table with a passed schema. 
# - If there are columns that exist in the schema that is not in the table, they will be added
# - If there are columns that exist in the table that is not in the schema, they will be dropped
#
# Note 1:
# Dropping columns require enabling Column Mapping feature on the delta table. 
# Doc: https://docs.microsoft.com/en-us/azure/databricks/delta/delta-column-mapping

# Note 2:
# Dropping columns will not delete the data from the table. It will apply a "soft-delete", meaning it only drop the column from the schema metadata. 
# To fully delete the column, the table has to be re-written. Databricks supports an efficient re-writing of the table using the `REORG` statment
# Doc: https://docs.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-reorg-table
############################# 

def evolve_schema(df, schema, table_name):
  diff = get_schema_diff(df, schema)

  add_columns(table_name, diff["added"])
  drop_columns(table_name, diff["dropped"])

def get_schema_diff(df, schema):
  fields = df.schema.fields
  table_schema = [f"{field.name} {field.dataType.simpleString()}" for field in fields]
  table_schema_lower = [c.lower() for c in table_schema]
  
  new_schema = [f"{c['name']} {c['type']}" for c in schema]
  new_schema_lower = [c.lower() for c in new_schema]
  
  return {
    "added": [c for c in new_schema if c.lower() not in table_schema_lower],
    "dropped": [c.split(" ")[0] for c in table_schema if c.lower() not in new_schema_lower]
  }
  
def add_columns(table_name, columns):
  if not len(columns):
    return 

  columns_string = ", ".join(columns)
  spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({columns_string})")

def drop_columns(table_name, columns):
  if not len(columns):
    return 
  
  columns_string = ", ".join(columns)
  spark.sql(f"ALTER TABLE {table_name} DROP COLUMNS ({columns_string})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demux functions

# COMMAND ----------

############################
# Function used to run multiple streams where each stream will handl many writes to tables.
#
# The `writeStream` calls are not blocking, so you can spin up as many streams concurrently (although the cluster will start to struggle if you run too many streams).
# The function will spins up one stream for each set of tables. The tables are grouped into sets to reduce the number of streams needed. 
# Then the `foreachBatch` function loops through each table in the set and does a batch write.
# If a lot of tables are packed into one job with this logic, the downside is that any failure will require the entire job to be restarted. 
# If there are some tables that have a lower SLA requirement or are considered more critical, they should be split out into their own streams or even their own jobs. 
# Other consideration when splitting to multiple streams can be table size and volume of change records. Largest tables should be in their own streams, and smaller tables can be packed into one stream.
############################

import json
from pyspark.sql.functions import lit

def demux_to_silver(list_of_grouped_tables, checkpoint_folder):
  schema_df = spark.read.table("cz_bronze.schema").cache()
  
  for table_list in list_of_grouped_tables:
    table_source_list = [table["source"] for table in table_list]
    table_names_list = [table["name"] for table in table_list]

    # Use the first table in the table list and the count of tables in the list as the checkpoint name and query name
    unique_identifier_string = "_".join([table_source_list[0], table_names_list[0], str(len(table_list))])

    # Read the data for this stream. Only pull data from the Bronze table for the set of tables in this list
    df = (
      spark
        .readStream
        .format("delta")
        .option("maxFilesPerTrigger", 200)
        .table("cz_bronze.demux")
        .filter(col("source_id").isin(table_source_list))
        .filter(col("table_name").isin(table_names_list))
    )

    query_name =  unique_identifier_string + "-cdc"
    checkpoint_path = checkpoint_folder +"/silver/"+ unique_identifier_string

    (
      df
        .writeStream
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(demux_batch(table_list, schemas_df))
        .trigger(availableNow=True)
        .queryName(query_name)
        .start()
    )

  schema_df.unpersist()

# COMMAND ----------

############################
# Functions used to divide and fanout the batch writes to multiple tables.
#
# It caches the input batch loops over the batch to write it to the target table as a Merge statement to acheive either SCD type 1 or SCD type 2 based on the table configuration.
# It parses the data in the batch using the schema found in the `schemas` table. 
# A batch can contain multiple tables and multiple versions of the schema for that table. The function will retrieve the schema matching the table name and version. 
# It will also combine all records for the same table and schema version to be parsed, evovled and written to the target table.
############################


import json 
from functools import reduce

from pyspark.sql.functions import col, split

############################
# The function uses the factory pattern where it returns an internal function on call that has a closure of all the variables defined inside when it was called. 
# This allow us to create "instances" of the function with different contextual varaibles. In this case, we use it to pass in the list of tables that will be processed in the batch.
############################
def demux_batch(table_list, schema_df):
  
  def __demux(batch_df, batch_id):
    cached_df = batch_df.cache()

    for table_config in table_list:
      df = cached_df.filter(f"source_id = '{table_config['source']}' AND table_name = '{table_config['name']}'")

      if not df.rdd.isEmpty():
        write_table(df, table_config, schema_df)

    cached_df.unpersist()
    
  return __demux

def write_table(df, table_config, schema_df):
  destination_table_name = f"cz_silver.{table_config['name']}"

  # Get all possible schema versions in the batch
  schema_versions = sorted([r["schema_version"] for r in df.select("schema_version").distinct().collect()])

  # Create the target table with the oldest schema available. Then evolve it later with any new changes
  initial_schema = get_table_schema(table_config, schema_df, schema_versions[0])
  create_table(destination_table_name, initial_schema)

  # handle each schema version as a separate batch to write
  for schema_version in schema_versions:
    table_schema = get_table_schema(table_config, schema_df, schema_version)
    df = map_to_schema(df.filter(f"schema_version = {schema_version}"), table_schema)
    
    evolve_schema(df, table_schema, destination_table_name)
    
    # Only hansles SCD type 1 for now, but can be extended to handle type 2 as well.
    if table_config["type"] == 1:
      write_scd_typ1(df, destination_table_name, table_config)
      
def get_table_schema(table_config, schema_df, schema_version):
  return json.loads(schema_df.select("schema").filter(f"source_id = '{table_config['source']}' AND table_name = '{table_config['name']}' AND version = {schema_version}").first()[0])

############################
# Function to map the CSV string into a dataframe
# Since the value is a string, it splits on the separator value (i.e. ',') and coverts the values into dataframe columns.
# It's important to map the value to the right column in the schema. 
# Hence, the schema has to have a column order defined as either it's location in an array or a property explicitly defining the order of the column in the the original CSV file.
############################
def map_to_schema(df, schema):
  column_names = [field["name"] for field in schema]
  df = df.withColumn("splitted_body", split(col("body"), ','))
  df = reduce(lambda data, field: data.withColumn(field["name"], col("splitted_body").getItem(field["order"]).cast(field["type"])), schema, df)
  return df.select(column_names)

def create_table(destination_table_name, schema):
  columns = ",".join([f"{column['name']} {column['type']}" for column in schema])

  spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {destination_table_name} ({columns})
    TBLPROPERTIES (
       'delta.columnMapping.mode' = 'name',
       'delta.minReaderVersion' = '2',
       'delta.minWriterVersion' = '5'
    )
  """)

def write_scd_typ1(df, destination_table_name, table_config):
  temp_table_name = f"temp_bronze_{table_config['source']}_{table_config['name']}"
  df.createOrReplaceTempView(temp_table_name)

  primary_keys_string = " AND ".join([f"s.{pk} = b.{pk}" for pk in table_config['primary_keys']])

  df._jdf.sparkSession().sql(f"""
    MERGE INTO {destination_table_name} s
    USING {temp_table_name} b
    ON {primary_keys_string}
    WHEN MATCHED AND s.operation_time < b.operation_time AND b.operation = 'U' 
      THEN UPDATE SET *
    WHEN MATCHED AND b.operation = 'D' 
      THEN DELETE
    WHEN NOT MATCHED AND b.operation in ('I', 'U') 
      THEN INSERT *
    """)
    

# COMMAND ----------

# MAGIC %md
# MAGIC # Execution

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tuning
# MAGIC
# MAGIC All the typical Delta table tuning best practices apply here - use optimized writes and partition larger tables if possible.  Make sure not to over-partition.
# MAGIC If the Demux process is just doing appends into the Silver tables, then a lot of tables can be grouped together in streams and performance will be fine.
# MAGIC If the Demux process is doing merges into the Silver tables, there are additional things to take into account.  Use low shuffle merge and skip data with matched predicates options in the cluster config to optimize merge performance
# MAGIC
# MAGIC #### Grouping fast tables
# MAGIC - Small tables with either low or hight amount of change records coming through.
# MAGIC - Large tables with low amount of change records coming through.  
# MAGIC
# MAGIC #### Grouping slow tables
# MAGIC - Larger tables with high amount of change records coming through.  These tables shouldn’t be grouped too much - depending on the size of the table only a couple should be grouped or you may need to have it be in its own stream. 
# MAGIC
# MAGIC
# MAGIC One other thing to consider is that the more streams you have, the more they will be fighting for resources on the cluster. This can be handled by distributing the load on multiple scheduler pools with FAIR scheduling

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "true") 

# COMMAND ----------

base_folder = "/FileStore/tables/kh/cz/demux-demo"
data_folder = base_folder+"/data"
checkpoint_folder = base_folder+"/checkpoint"

dbutils.fs.rm(data_folder, True)
dbutils.fs.rm(checkpoint_folder, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create fake data
# MAGIC
# MAGIC Creating some sample tables to work with.
# MAGIC This will create two tables `customers` and `transactions` with 100k rows of random data.

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

from pyspark.sql import functions as F
from faker import Faker
from collections import OrderedDict 
import uuid
  
fake = Faker()
fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%Y-%m-%d %H:%M:%S"))
fake_address = F.udf(fake.street_address)
operations = OrderedDict([("I", 0.5),("D", 0.1),("U", 0.3),(None, 0.01)])
fake_operation = F.udf(lambda:fake.random_elements(elements=operations, length=1)[0])
fake_id = F.udf(lambda: str(uuid.uuid4()))

customers_path = data_folder+"/layer=landing/source=peoplesoft/collection=customers"

customers_df = (
    spark
      .range(0, 100000)
      .withColumn("id", fake_id())
      .withColumn("firstname", fake_firstname())
      .withColumn("lastname", fake_lastname())
      .withColumn("email", fake_email())
      .withColumn("address", fake_address())
      .withColumn("operation", fake_operation())
      .withColumn("operation_time", fake_date())
)

(
  customers_df
    .repartition(10)
    .write
    .format("csv")
    .mode("overwrite")
    .save(customers_path)
)

################################################################################ 
  
transaction_path = data_folder+"/layer=landing/source=mars/collection=transactions"

transaction_df = (
  spark
    .range(0, 100000)
    .withColumn("id", fake_id())
    .withColumn("transaction_time", fake_date())
    .withColumn("amount", F.round(F.rand()*1000))
    .withColumn("item_count", F.round(F.rand()*10))
    .withColumn("operation", fake_operation())
    .withColumn("operation_time", fake_date())
    .withColumn("customer_id", fake_id())
)

(
  transaction_df
    .repartition(10)
    .write
    .format("csv")
    .mode("overwrite")
    .save(transaction_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multiplexing to Bronze Execution

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS cz_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists cz_bronze.demux

# COMMAND ----------

multiplex_to_bronze(data_folder, checkpoint_folder, 'csv')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail cz_bronze.demux

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cz_bronze.demux limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Schema Table
# MAGIC
# MAGIC The schema table will holds all schemas for all tables to be ingested with the version for each schema changes. This will be used by the streaming demultiplexing functions to be able to parse the data correctly using the schema they were written with. Here, the schema is stored as a JSON string that holds for each column, the name, type and order of the column in the schema to be used to correctly map the column to the proper value in the CSV record. You can extend the JSON object to hold more properties around each column, such as comments.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS cz_bronze.schema (
# MAGIC   source_id string,
# MAGIC   table_name string,
# MAGIC   schema string,
# MAGIC   primary_keys string,
# MAGIC   version string,
# MAGIC   added_time timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE cz_bronze.schema 
# MAGIC VALUES
# MAGIC ("peoplesoft", "customers", '[{"name": "id", "type": "string", "order": 0}, {"name": "firstname", "type": "string", "order": 1}, {"name": "lastname", "type": "string", "order": 2}, {"name": "email", "type": "string", "order": 3}, {"name": "address", "type": "string", "order": 4}, {"name": "operation", "type": "string", "order": 5}, {"name": "operation_time", "type": "timestamp", "order": 6}]', "id", "1", current_timestamp()),
# MAGIC ("mars", "transactions", '[{"name": "id", "type": "string", "order": 0}, {"name": "transaction_time", "type": "timestamp", "order": 1}, {"name": "amount", "type": "int", "order": 2}, {"name": "item_count", "type": "int", "order": 3}, {"name": "operation", "type": "string", "order": 4}, {"name": "operation_time", "type": "timestamp", "order": 5}, {"name": "customer_id", "type": "string", "order": 6}]', "id", "1", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demultiplexing to Silver Execution

# COMMAND ----------

tables = [{
    'name': 'customers',
    'primary_keys': ['id'],
    'layer': 'landing',
    'source': 'peoplesoft',
    'type': 1
},{
    'name': 'transactions',
    'primary_keys': ['id'],
    'layer': 'landing',
    'source': 'mars',
    'type': 1
}]

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS cz_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists cz_silver.customers;
# MAGIC drop table if exists cz_silver.transactions

# COMMAND ----------

list_of_grouped_tables = [tables]

demux_to_silver(list_of_grouped_tables, checkpoint_folder)

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in cz_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cz_silver.customers limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cz_silver.transactions limit 10
