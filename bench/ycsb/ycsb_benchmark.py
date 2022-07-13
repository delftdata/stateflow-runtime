import asyncio
import os
import random
import logging
from typing import Type

import pandas as pd
import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis

from ycsb.functions import ycsb
from ycsb.functions.graph import ycsb_operator, g
from ycsb.util.zipfian_generator import ZipfGenerator


class YcsbBenchmark:
    UNIVERSALIS_HOST: str = 'localhost'
    UNIVERSALIS_PORT: int = 8886
    KAFKA_URL = 'localhost:9093'
    universalis: Universalis

    N_ROWS = 10000

    def __init__(self):
        self.keys: list[int] = list(range(self.N_ROWS))
        self.operations: list[Type] = [ycsb.Read, ycsb.Update]
        self.operation_counts: dict[Type, int] = {transaction: 0 for transaction in self.operations}
        self.operation_mix: list[float] = [0.5, 0.5]

    async def initialise(self):
        self.universalis = Universalis(
            self.UNIVERSALIS_HOST,
            self.UNIVERSALIS_PORT,
            ingress_type=IngressTypes.KAFKA,
            kafka_url=self.KAFKA_URL
        )

        await self.universalis.submit(g)
        await asyncio.sleep(2)
        logging.info('Graph submitted')

    async def insert_records(self):
        logging.info('Inserting')
        tasks = []

        for i in self.keys:
            tasks.append(self.universalis.send_kafka_event(
                operator=ycsb_operator,
                key=i,
                function=ycsb.Insert,
                params=(i,))
            )
        await asyncio.gather(*tasks)
        logging.info(f'All {self.N_ROWS} Records Inserted')
        await asyncio.sleep(2)

    async def run_transaction_mix(self):
        logging.info('Running Transaction Mix')
        zipf_gen = ZipfGenerator(items=self.N_ROWS)
        tasks = []

        for i in range(self.N_ROWS):
            key = self.keys[next(zipf_gen)]
            op = random.choices(self.operations, weights=self.operation_mix, k=1)[0]
            self.operation_counts[op] += 1

            if op == ycsb.Transfer:
                key2 = self.keys[next(zipf_gen)]
                while key2 == key:
                    key2 = self.keys[next(zipf_gen)]
                tasks.append(self.universalis.send_kafka_event(ycsb_operator, key, op, (key, key2)))
            else:
                tasks.append(self.universalis.send_kafka_event(ycsb_operator, key, op, (key,)))

        responses = await asyncio.gather(*tasks)

        logging.info(self.operation_counts)
        logging.info('Transaction Mix Complete')

        await asyncio.sleep(1)
        return responses

    async def cleanup(self):
        await self.universalis.close()

    def generate_request_data(self, responses):
        timestamped_request_ids = {}

        for request_id, timestamp in responses:
            timestamped_request_ids[request_id] = timestamp
            filename = os.getcwd() + '/requests.csv'

        pd.DataFrame(timestamped_request_ids.items(), columns=['request_id', 'timestamp']).to_csv(
            filename,
            index=False
            )

    async def run(self):
        await self.initialise()
        await self.insert_records()
        responses = await self.run_transaction_mix()
        await self.cleanup()
        self.generate_request_data(responses)