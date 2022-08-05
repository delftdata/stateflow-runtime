import asyncio
import time
from asyncio import Event, Lock

import uvloop
from aiokafka import AIOKafkaConsumer
from kafka.errors import UnknownTopicOrPartitionError, KafkaConnectionError

from common.logging import logging
from universalis.common.serialization import msgpack_deserialization


class BenchmarkConsumer:
    # Time to wait after last message before stopping connection to the Kafka queue
    LAST_MESSAGE_TIMEOUT = 5
    consumer: AIOKafkaConsumer

    def __init__(self):
        self.consumer_ready_event: asyncio.Event() = Event()
        self.timeout_event: asyncio.Event = Event()
        self.last_message_time_lock: asyncio.Lock = Lock()
        self.last_message_time: float = float('inf')
        self.responses_lock: asyncio.Lock = Lock()
        self.responses: list[dict] = []

    async def get_run_responses(self):
        await self.timeout_event.wait()

        async with self.responses_lock:
            responses = self.responses
            self.responses = []
            return responses

    async def stop(self):
        await self.consumer.stop()

    async def check_for_timeout(self):
        while True:
            if self.timeout_event.is_set():
                self.timeout_event.clear()

            async with self.last_message_time_lock:
                if time.time() - self.last_message_time >= self.LAST_MESSAGE_TIMEOUT:
                    logging.info(f'{self.LAST_MESSAGE_TIMEOUT} has passed, outputting responses')
                    self.timeout_event.set()
                    self.last_message_time: float = float('inf')

            await asyncio.sleep(1)

    async def consume_messages(self):
        logging.warning("Consuming...")
        try:
            self.consumer_ready_event.set()

            async for msg in self.consumer:
                async with self.responses_lock:
                    self.responses += [{
                        'request_id': msg.key,
                        'response': msg.value,
                        'timestamp': msg.timestamp,
                    }]

                async with self.last_message_time_lock:
                    self.last_message_time = time.time()

                await asyncio.sleep(0.001)
        except:
            self.consumer_ready_event.clear()
            await self.consumer.stop()

    async def run(self):
        self.consumer: AIOKafkaConsumer = AIOKafkaConsumer(
            'universalis-egress',
            key_deserializer=msgpack_deserialization,
            value_deserializer=msgpack_deserialization,
            bootstrap_servers='localhost:9093'
        )

        while True:
            # start the kafka consumer
            try:
                await self.consumer.start()
            except (UnknownTopicOrPartitionError, KafkaConnectionError):
                time.sleep(1)
                continue
            break

        asyncio.create_task(self.consume_messages())
        asyncio.create_task(self.check_for_timeout())


if __name__ == "__main__":
    uvloop.install()
    bc = BenchmarkConsumer()
    asyncio.run(bc.run())
