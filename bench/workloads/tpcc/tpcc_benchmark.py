import asyncio
import logging

from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis

from workloads.tpcc.functions.graph import g
from workloads.tpcc.loader import Loader
from workloads.tpcc.util.scale_parameters import make_default


class TpccBenchmark:
    UNIVERSALIS_HOST: str = 'localhost'
    UNIVERSALIS_PORT: int = 8886
    KAFKA_URL = 'localhost:9093'

    universalis: Universalis
    loader: Loader

    def __init__(self):
        self.scale_parameters = make_default(1)

    async def initialise(self):
        self.universalis = Universalis(
            self.UNIVERSALIS_HOST,
            self.UNIVERSALIS_PORT,
            ingress_type=IngressTypes.KAFKA,
            kafka_url=self.KAFKA_URL
        )

        self.loader = Loader(make_default, self.scale_parameters.warehouses, self.universalis)

        await self.universalis.submit(g)
        await asyncio.sleep(2)
        logging.info('Graph submitted')

    async def insert_records(self):
        pass

    async def run_transaction_mix(self):
        pass

    async def cleanup(self):
        pass

    def generate_request_data(self, responses):
        pass

    async def run(self):
        await self.initialise()
        await self.insert_records()
        responses = await self.run_transaction_mix()
        await self.cleanup()
        self.generate_request_data(responses)
