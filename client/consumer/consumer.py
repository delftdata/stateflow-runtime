import asyncio
import os
import time
from asyncio import Event, Lock

import uvloop
from aiokafka import AIOKafkaConsumer

from common.logging import logging
from universalis.common.serialization import msgpack_deserialization

NUM_CONSUMERS: int = int(os.environ['NUM_CONSUMERS'])


class BenchmarkConsumer:
    # Time to wait after last message before stopping connection to the Kafka queue
    LAST_MESSAGE_TIMEOUT = 5
    consumer: AIOKafkaConsumer

    def __init__(self):
        self.timeout_event: asyncio.Event = Event()
        self.last_message_time_lock: asyncio.Lock = Lock()
        self.last_message_time: float = float('inf')
        self.consumer_ready_events: dict[int, asyncio.Event()] = {}
        self.responses: dict[int, list[dict]] = {}
        self.response_locks: dict[int, asyncio.Lock] = {}
        self.consumers: dict[int, AIOKafkaConsumer] = {}

    async def get_run_responses(self):
        await self.timeout_event.wait()
        results = []

        for key, response in self.responses.items():
            async with self.response_locks[key]:
                results += response
                self.responses[key] = []

        return results

    async def stop(self):
        for key, consumer in self.consumers.items():
            await consumer.stop()

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

    async def add_response_to_records(self, number: int, msg):
        logging.info(f'consumer {number} consumed')
        self.responses[number] += [{
            'request_id': msg.key,
            'response': msg.value,
            'timestamp': msg.timestamp,
        }]

        async with self.last_message_time_lock:
            self.last_message_time = time.time()

    async def consume_messages(self, number: int, consumer: AIOKafkaConsumer):
        logging.warning("Consuming...")

        try:
            self.consumer_ready_events[number].set()

            async for msg in consumer:
                async with self.response_locks[number]:
                    self.responses[number] += [{
                        'request_id': msg.key,
                        'response': msg.value,
                        'timestamp': msg.timestamp,
                    }]

                async with self.last_message_time_lock:
                    self.last_message_time = time.time()
                asyncio.create_task(self.add_response_to_records(number, msg))
        except:
            self.consumer_ready_events[number].clear()
            await consumer.stop()

    async def run(self):
        for i in range(1, NUM_CONSUMERS + 1):
            consumer: AIOKafkaConsumer = AIOKafkaConsumer(
                'universalis-egress',
                key_deserializer=msgpack_deserialization,
                value_deserializer=msgpack_deserialization,
                bootstrap_servers='localhost:9093',
                group_id=f"universalis-egress-consumer-group"
            )

            self.responses[i] = []
            self.response_locks[i] = asyncio.Lock()

            self.consumers[i] = consumer
            self.consumer_ready_events[i] = asyncio.Event()
            await consumer.start()

        for i in range(1, NUM_CONSUMERS + 1):
            asyncio.create_task(self.consume_messages(i, self.consumers[i]))

        asyncio.create_task(self.check_for_timeout())


if __name__ == "__main__":
    uvloop.install()
    bc = BenchmarkConsumer()
    asyncio.run(bc.run())
