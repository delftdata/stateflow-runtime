import asyncio
import os
import time
import logging
from asyncio import Event, Lock

import pandas as pd
import uvloop
from aiokafka import AIOKafkaConsumer
from universalis.common.serialization import msgpack_deserialization

logging.basicConfig(level=logging.INFO)


class BenchmarkConsumer:
    # Time to wait after last message before stopping connection to the Kafka queue
    LAST_MESSAGE_TIMEOUT = 5
    consumer: AIOKafkaConsumer

    def __init__(self):
        self.timeout_event: asyncio.Event = Event()
        self.last_message_time_lock: asyncio.Lock = Lock()
        self.last_message_time: float = float('inf')
        self.records: list[tuple] = []

    async def last_message_timeout(self):
        while True:
            async with self.last_message_time_lock:
                if time.time() - self.last_message_time >= self.LAST_MESSAGE_TIMEOUT:
                    logging.info(f'{self.LAST_MESSAGE_TIMEOUT} has passed, ending consumer')
                    self.timeout_event.set()
                    break

            await asyncio.sleep(1)

    async def consume_messages(self):
        logging.info("Consuming...")
        try:
            async for msg in self.consumer:
                self.records.append((msg.key, msg.value, msg.timestamp))
                async with self.last_message_time_lock:
                    self.last_message_time = time.time()

                await asyncio.sleep(0.001)
        except:
            await self.consumer.stop()

    async def run(self):
        self.consumer: AIOKafkaConsumer = AIOKafkaConsumer(
            'universalis-egress',
            key_deserializer=msgpack_deserialization,
            value_deserializer=msgpack_deserialization,
            bootstrap_servers='localhost:9093'
        )

        await self.consumer.start()
        asyncio.create_task(self.consume_messages())
        asyncio.create_task(self.last_message_timeout())
        await self.timeout_event.wait()

        # Will leave consumer group; perform autocommit if enabled.
        logging.info("Writing...")
        await self.consumer.stop()

        responses = pd.DataFrame.from_records(self.records, columns=['request_id', 'response', 'timestamp'])
        responses_filename = os.getcwd() + '/responses.csv'
        responses.to_csv(responses_filename, index=False)
