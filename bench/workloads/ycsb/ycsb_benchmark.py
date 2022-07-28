import asyncio
import os.path
import random
from typing import Type

import pandas as pd
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis

from common.logging import logging
from workloads.ycsb.functions import ycsb
from workloads.ycsb.functions.graph import ycsb_operator, g
from workloads.ycsb.util.zipfian_generator import ZipfGenerator


class YcsbBenchmark:
    UNIVERSALIS_HOST: str = 'localhost'
    UNIVERSALIS_PORT: int = 8886
    KAFKA_URL = 'localhost:9093'
    universalis: Universalis

    N_ROWS = 100000

    def __init__(self):
        self.keys: list[int] = list(range(self.N_ROWS))
        self.operations: list[Type] = [ycsb.Read, ycsb.Update, ycsb.Transfer]
        self.operation_counts: dict[Type, int] = {transaction: 0 for transaction in self.operations}
        self.operation_mix: list[float] = [0, 0, 1]
        self.batch_size: int = 10000

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
            tasks.append(
                self.universalis.send_kafka_event(
                    operator=ycsb_operator,
                    key=i,
                    function=ycsb.Insert,
                    params=(i,)
                )
            )

            if len(tasks) >= self.batch_size:
                await asyncio.gather(*tasks)
                tasks = []

        if len(tasks) > 0:
            await asyncio.gather(*tasks)

        logging.info(f'All {self.N_ROWS} Records Inserted')
        await asyncio.sleep(2)

    async def run_transaction_mix(self):
        logging.info('Running Transaction Mix')
        zipf_gen = ZipfGenerator(items=self.N_ROWS)
        tasks = []
        responses = []

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

            if len(tasks) >= self.batch_size:
                task_results = await asyncio.gather(*tasks)
                responses += task_results
                tasks = []

        if len(tasks) > 0:
            task_results = await asyncio.gather(*tasks)
            responses += task_results

        logging.info(self.operation_counts)
        logging.info('Transaction Mix Complete')

        await asyncio.sleep(1)
        return responses

    async def cleanup(self):
        await self.universalis.close()

    def generate_request_data(self, responses):
        timestamped_request_ids = {}

        for response in responses:
            request_id, timestamp = response
            timestamped_request_ids[request_id] = timestamp

        requests_filename = os.path.join('./results', 'requests.csv')
        requests = pd.DataFrame(timestamped_request_ids.items(), columns=['request_id', 'timestamp'])
        requests.to_csv(requests_filename, index=False)

    async def run(self):
        await self.initialise()
        await self.insert_records()
        responses = await self.run_transaction_mix()
        await self.cleanup()
        self.generate_request_data(responses)