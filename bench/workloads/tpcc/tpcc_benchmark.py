import asyncio
import os
from abc import abstractmethod

import pandas as pd
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis

import workloads
from common.logging import logging
from workloads.tpcc.functions.graph import g
from workloads.tpcc.runtime.executor import Executor
from workloads.tpcc.runtime.loader import Loader
from workloads.tpcc.util import rand, nurand
from workloads.tpcc.util.benchmark_parameters import BenchmarkParameters
from workloads.tpcc.util.scale_parameters import make_with_scale_factor


class TpccBenchmark:
    UNIVERSALIS_HOST: str = 'localhost'
    UNIVERSALIS_PORT: int = 8886
    KAFKA_URL = 'localhost:9093'

    universalis: Universalis
    loader: Loader
    executor: Executor

    def __init__(self):
        self.benchmark_parameters = BenchmarkParameters(benchmark_duration=1)
        self.scale_parameters = make_with_scale_factor(1, 100)
        self.nu_rand = rand.set_nu_rand(nurand.make_for_load())

    async def initialise(self):
        self.universalis = Universalis(
            self.UNIVERSALIS_HOST,
            self.UNIVERSALIS_PORT,
            ingress_type=IngressTypes.KAFKA,
            kafka_url=self.KAFKA_URL
        )

        self.loader = Loader(self.benchmark_parameters, self.scale_parameters, [1], self.universalis)
        self.executor = Executor(self.benchmark_parameters, self.scale_parameters, self.universalis)

        await self.universalis.submit(g, (workloads,))
        logging.info('Graph submitted')

    async def insert_records(self):
        await self.loader.execute()

    async def run_transaction_mix(self):
        return await self.executor.execute_transactions()

    async def cleanup(self):
        await self.universalis.close()

    @abstractmethod
    def generate_request_data(self, responses):
        timestamped_request_ids = {}

        for response in responses:
            request_id, timestamp = response
            timestamped_request_ids[request_id] = timestamp

        requests_filename = os.path.join('./results', 'requests.csv')
        requests = pd.DataFrame(timestamped_request_ids.items(), columns=['request_id', 'timestamp'])
        requests.to_csv(requests_filename, index=False)

    async def run(self):
        logging.info('Initialising...')
        await self.initialise()
        logging.info('Initialised')
        await asyncio.sleep(2)
        logging.info('Inserting records...')
        await self.insert_records()
        logging.info('Finished inserting')
        await asyncio.sleep(2)
        logging.info('Running transaction mix...')
        requests = await self.run_transaction_mix()
        logging.info('Finished running transaction mix')
        await asyncio.sleep(2)
        logging.info('Cleaning up...')
        await self.cleanup()
        logging.info('Cleaned up')
        await asyncio.sleep(2)
        logging.info('Generating request data...')
        self.generate_request_data(requests)
        logging.info('Generated request data')
