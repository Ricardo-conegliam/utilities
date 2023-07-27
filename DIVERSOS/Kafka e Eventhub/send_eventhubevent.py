# Databricks notebook source
import asyncio
#from azure.eventhub.aio import EventHubProducerClient
import com.microsoft.azure.eventhubs._
from azure.eventhub import EventData

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://conegliamnamespace02.servicebus.windows.net/;SharedAccessKeyName=totalsas;SharedAccessKey=uH+UeeXBnmUdGOVnbZd+FqT+LTv0/b24hJ+WC6JIHIs=", eventhub_name="topico1")
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        event_data_batch.add(EventData('First event '))
        event_data_batch.add(EventData('Second event'))
        event_data_batch.add(EventData('Third event'))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

loop = asyncio.get_event_loop()
loop.run_until_complete(run())


# COMMAND ----------


