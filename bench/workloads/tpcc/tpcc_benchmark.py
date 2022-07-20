import asyncio

from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis

from common.logging import logging
from workloads.tpcc.functions.graph import g
from workloads.tpcc.runtime.executor import Executor
from workloads.tpcc.runtime.loader import Loader
from workloads.tpcc.util.scale_parameters import make_with_scale_factor


class TpccBenchmark:
    UNIVERSALIS_HOST: str = 'localhost'
    UNIVERSALIS_PORT: int = 8886
    KAFKA_URL = 'localhost:9093'

    universalis: Universalis
    loader: Loader
    executor: Executor

    def __init__(self):
        self.scale_parameters = make_with_scale_factor(1, 100)

    async def initialise(self):
        self.universalis = Universalis(
            self.UNIVERSALIS_HOST,
            self.UNIVERSALIS_PORT,
            ingress_type=IngressTypes.KAFKA,
            kafka_url=self.KAFKA_URL
        )

        self.loader = Loader(self.scale_parameters, [1], self.universalis)
        self.executor = Executor(self.scale_parameters, self.universalis)

        await self.universalis.submit(g)
        await asyncio.sleep(2)
        logging.info('Graph submitted')

    async def insert_records(self):
        await self.loader.execute()

    async def run_transaction_mix(self):
        await self.executor.execute_transaction()

    async def cleanup(self):
        await self.universalis.close()

    def generate_request_data(self, responses):
        pass

    async def run(self):
        await self.initialise()
        await self.insert_records()
        responses = await self.run_transaction_mix()
        await self.cleanup()
        self.generate_request_data(responses)
