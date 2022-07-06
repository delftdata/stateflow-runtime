import asyncio

import pandas as pd
import uvloop
from aiokafka import AIOKafkaConsumer
from universalis.common.serialization import msgpack_deserialization

# Time to wait before stopping connection to the Kafka queue
CONSUME_TIMEOUT = 60


async def generate_records(consumer: AIOKafkaConsumer, records: list):
    async for msg in consumer:
        records.append((msg.key, msg.value, msg.timestamp))


async def consume():
    records = []
    consumer = AIOKafkaConsumer(
        'universalis-egress',
        key_deserializer=msgpack_deserialization,
        value_deserializer=msgpack_deserialization,
        bootstrap_servers='localhost:9093')

    await consumer.start()

    try:
        # Consume messages
        print("Consuming...")
        await asyncio.wait_for(generate_records(consumer, records), CONSUME_TIMEOUT)
    except asyncio.TimeoutError:
        print("Consuming Finished")
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        print("Writing...")
        await consumer.stop()

        requests = pd.read_csv('requests.csv')
        responses = pd.DataFrame.from_records(records, columns=['request_id', 'response', 'timestamp'])

        joined = pd.merge(requests, responses, on='request_id', how='outer')
        joined['runtime'] = (joined['timestamp_y'] - joined['timestamp_x']).dropna()

        joined.to_csv('responses.csv', index=False)

if __name__ == "__main__":
    uvloop.install()
    asyncio.run(consume())
