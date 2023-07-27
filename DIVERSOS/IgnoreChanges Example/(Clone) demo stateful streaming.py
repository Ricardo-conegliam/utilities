# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Template Stateful Streaming - Python
# MAGIC
# MAGIC Based on a blog entry -> [here](https://www.databricks.com/blog/2022/10/18/python-arbitrary-stateful-processing-structured-streaming.html)

# COMMAND ----------

#small clusters should have few partitions
spark.conf.set("spark.sql.shuffle.partitions","8")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- restart our demo table
# MAGIC drop table if exists  leandro_events

# COMMAND ----------

# MAGIC %md ## Generating fake data

# COMMAND ----------

#Generating some random event data, events will arrive out of order

import dbldatagen as dg
from pyspark.sql.types import FloatType, IntegerType, StringType

row_count =  200

testDataSpec = (dg.DataGenerator(spark, name="events", rows=row_count,
                                  partitions=4, randomSeedMethod='hash_fieldname', 
                                  verbose=False)
                   .withIdOutput()
                   .withColumn("customer", StringType(), values=[1, 3, 5, 7, 9])
                   .withColumn("event", StringType(), values=['event 1', 'event 2', 'event 3'],random=True)
                   .withColumn("value", FloatType(), minValue=100.00, maxValue=2500.99, random=True)
                   )

df = testDataSpec.build()

delta_table = 'leandro_events'

# COMMAND ----------

from pyspark.sql import functions as f

#append the generated dataframe above to a delta table
new_events = (df
                .withColumn("event_time",f.current_timestamp())
                .filter(f.expr("not(customer = 9 and event = 'event 3')")) # remove all rows where customer number 9 receives a "event 3", for the final step of this demo
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "false")
                .saveAsTable(delta_table))

# COMMAND ----------

# MAGIC %sql select * from leandro_events where customer = 9 and event = 'event 3'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining the streaming input

# COMMAND ----------

#now, we will stream data from that delta table that we just created
stream = spark.readStream.table(delta_table)

# COMMAND ----------

# predefine the output schema and state schema for the process

output_schema = """
                    customer STRING,
                    event_list ARRAY<STRING>
                """

state_schema = """
                   event_list ARRAY<STRING>
               """

# COMMAND ----------

# MAGIC %md ## Main logic - aggregating pandas function

# COMMAND ----------

#code main logic, how to agreggate and deal with the custom state

import pandas as pd
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from typing import Tuple, Iterator

def updateAcrossEvents(key: Tuple[str], pdfs: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
    if state.hasTimedOut:
        #infinite state for this demo, this can be activated by choosing `ProcessingTimeTimeout` in the GroupStateTimeout parameter
        state.remove()
    else:
        
        #fetch the keys from the group
        (group_key,) = key 

        #generate or initialize metrics
        event_list = list()
        
        #aggregate current batch
        for pdf in pdfs:
          event_list.extend(pdf["event"].unique())
                        
        if state.exists:

          state_dict = dict(zip(state._value_schema.fieldNames(),state.get))   

          #for groups with an defined state, recalculate metrics using the state data         
          event_list = list(set(event_list + state_dict["event_list"]))

        #save in progress aggregation to state
        state.update((event_list,))

        #Set the timeout as 24 hours. Only when applicable
        #state.setTimeoutDuration(60*60*24*100)
        
        #send row to output when it meets a criteria
        if "event 3" in event_list:
          state.remove()
          yield pd.DataFrame({"customer": [group_key],"event_list":[event_list]})

# COMMAND ----------

# MAGIC %md ## Results -  What's happening?
# MAGIC
# MAGIC </br>
# MAGIC
# MAGIC * Customers 1,3,5 and 7 will appear on the result
# MAGIC * Customer 9 won't show up, due to the function criteria, but its state is still waiting for the `event 3`
# MAGIC * After the results appeared, run the SQL cell below and see what happens in the results

# COMMAND ----------

#executing the function `updateAcrossEvents` created above

event_collection = stream.groupBy(stream["customer"]).applyInPandasWithState(
    updateAcrossEvents,
    output_schema,
    state_schema,
    "append",
    GroupStateTimeout.NoTimeout,
)

display(event_collection)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- run this after some minutes
# MAGIC insert into leandro_events values (5000,9,"event 3", 30, current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **OBS: Avoid to run stateful streaming with infinite timeouts or very long ones, the state data is stored in memory and can become quite large, leading to OOM issues**

# COMMAND ----------

# MAGIC %md **Also, beware - Python Stateful Streaming doesn't support nested types yet**
# MAGIC ```
# MAGIC 102 /databricks/spark/python/pyspark/sql/pandas/types.py in to_arrow_type(dt)
# MAGIC 103             raise TypeError("Nested StructType not supported in conversion to Arrow")
# MAGIC ```
