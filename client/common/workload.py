import asyncio
from abc import abstractmethod, ABC

import workloads
from common.logging import logging
from consumer.consumer import BenchmarkConsumer
from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis


class Workload(ABC):
    UNIVERSALIS_HOST: str = 'localhost'
    UNIVERSALIS_PORT: int = 8886
    KAFKA_URL = 'localhost:9093'
    consumer: BenchmarkConsumer
    graph: StateflowGraph
    run_number: int

    def __init__(self):
        self.params: dict = self.parse_benchmark_parameters()
        self.num_runs: int = self.params['num_runs']
        self.num_concurrent_tasks: int = self.params['num_concurrent_tasks']
        self.operation_mix: list[float] = self.params['operation_mix']
        self.requests: list[dict] = []
        self.responses: list[dict] = []
        self.universalis = Universalis(
            self.UNIVERSALIS_HOST,
            self.UNIVERSALIS_PORT,
            ingress_type=IngressTypes.KAFKA,
            kafka_url=self.KAFKA_URL
        )

    async def init_consumer(self):
        self.consumer: BenchmarkConsumer = BenchmarkConsumer()
        asyncio.create_task(self.consumer.run())
        await self.consumer.consumer_ready_event.wait()

    async def init_universalis(self):
        await self.universalis.submit(self.graph, (workloads,))
        await asyncio.sleep(2)

    @staticmethod
    @abstractmethod
    def parse_benchmark_parameters() -> dict:
        pass

    @abstractmethod
    async def run_transaction_mix(self):
        pass

    @abstractmethod
    async def insert_records(self):
        pass

    @abstractmethod
    async def run_validation(self):
        pass

    @abstractmethod
    def generate_metrics(self):
        pass

    async def init_run(self, run_number):
        pass

    async def cleanup_run(self):
        pass

    async def run(self):
        await self.init_consumer()
        await self.init_universalis()

        for run_number in range(self.num_runs):
            logging.info(f'Run {run_number} - Initialising run...')
            await self.init_run(run_number)
            logging.info(f'Run {run_number} - Initialised')
            await asyncio.sleep(1)

            logging.info(f'Run {run_number} - Inserting records...')
            await self.insert_records()
            logging.info(f'Run {run_number} - Finished inserting')
            await asyncio.sleep(2)

            logging.info(f'Run {run_number} - Running transaction mix...')
            await self.run_transaction_mix()
            logging.info(f'Run {run_number} - Finished running transaction mix')
            await asyncio.sleep(2)

            logging.info(f'Run {run_number} - Running validation...')
            await self.run_validation()
            logging.info(f'Run {run_number} - Finished validation')
            await asyncio.sleep(2)

            logging.info(f'Run {run_number} - Cleaning up...')
            await self.cleanup_run()
            logging.info(f'Run {run_number} - Cleaned up')

            self.responses += await self.consumer.get_run_responses()
            logging.info(f'Run {run_number} - Complete')

        await self.consumer.stop()
        await self.universalis.close()

        await asyncio.sleep(2)
        self.generate_metrics()
