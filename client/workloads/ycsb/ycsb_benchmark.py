import asyncio
import configparser
import json
import random

import workloads
from common.logging import logging
from consumer.consumer import BenchmarkConsumer
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis
from workloads.ycsb.functions import ycsb
from workloads.ycsb.functions.graph import ycsb_operator, g
from workloads.ycsb.util import calculate_metrics
from workloads.ycsb.util.zipfian_generator import ZipfGenerator


class YcsbBenchmark:
    UNIVERSALIS_HOST: str = 'localhost'
    UNIVERSALIS_PORT: int = 8886
    KAFKA_URL = 'localhost:9093'
    consumer: BenchmarkConsumer()
    universalis: Universalis
    operations: list[str] = ['Read', 'Update', 'Transfer']
    run_number: int

    def __init__(self):
        self.params = self.parse_benchmark_parameters()
        self.num_runs = self.params['num_runs']
        self.batch_size: int = self.params['batch_size']
        self.num_rows: int = self.params['num_rows']
        self.num_operations: int = self.params['num_operations']
        self.num_concurrent_tasks: int = self.params['num_concurrent_tasks']
        self.operation_mix: list[float] = self.params['operation_mix']
        self.keys: list[int] = list(range(self.num_rows))
        self.requests: list[dict] = []
        self.responses: list[dict] = []
        self.balances: dict[(int, str), dict[str, int]] = {}

    @staticmethod
    def parse_benchmark_parameters():
        config = configparser.ConfigParser()
        config.read('workload.ini')

        return {
            'workload': 'YCSB+T',
            'num_runs': int(config['Benchmark']['num_runs']),
            'num_rows': int(config['Benchmark']['num_rows']),
            'num_operations': int(config['Benchmark']['num_operations']),
            'num_concurrent_tasks': int(config['Benchmark']['num_concurrent_tasks']),
            'batch_size': int(config['Benchmark']['batch_size']),
            'operation_mix': json.loads(config['Benchmark']['operation_mix'])
        }

    async def initialise_run(self, run_number: int):
        self.run_number = run_number

        for key in self.keys:
            self.balances[(self.run_number, str(key))] = {'expected': 100, 'received': 0}

    async def insert_records(self):
        async_request_responses = []

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

            if len(tasks) == self.batch_size:
                async_request_responses += await asyncio.gather(*tasks)
                tasks = []

        if len(tasks) > 0:
            async_request_responses += await asyncio.gather(*tasks)

        for request in async_request_responses:
            request_id, timestamp = request
            self.requests += [{
                'run_number': self.run_number,
                'request_id': request_id,
                'function': 'Insert',
                'stage': 'insertion',
                'timestamp': timestamp
            }]

    async def run_transaction_mix(self):
        zipf_gen = ZipfGenerator(items=self.num_rows)
        tasks = []
        async_request_responses = []
        requests_meta: dict[int, str] = {}

        for i in range(self.num_operations):
            key = self.keys[next(zipf_gen)]
            op = random.choices(self.operations, weights=self.operation_mix, k=1)[0]
            requests_meta[i] = op

            if op == 'Transfer':
                key2 = self.keys[next(zipf_gen)]
                while key2 == key:
                    key2 = self.keys[next(zipf_gen)]

                self.balances[(self.run_number, str(key))]['expected'] -= 1
                self.balances[(self.run_number, str(key2))]['expected'] += 1

                tasks.append(self.universalis.send_kafka_event(ycsb_operator, key, op, (key, key2)))
            elif op == 'Update':
                self.balances[(self.run_number, str(key))]['expected'] += 1
                tasks.append(self.universalis.send_kafka_event(ycsb_operator, key, op, (key,)))
            else:
                tasks.append(self.universalis.send_kafka_event(ycsb_operator, key, op, (key,)))

            if len(tasks) == self.batch_size:
                async_request_responses += await asyncio.gather(*tasks)
                tasks = []

        if len(tasks) > 0:
            async_request_responses += await asyncio.gather(*tasks)

        for i, request in enumerate(async_request_responses):
            request_id, timestamp = request
            self.requests += [{
                'run_number': self.run_number,
                'request_id': request_id,
                'function': requests_meta[i],
                'stage': 'transaction_mix',
                'timestamp': timestamp
            }]

    async def run_validation(self):
        tasks = []
        async_request_responses = []

        for i in self.keys:
            tasks.append(
                self.universalis.send_kafka_event(
                    operator=ycsb_operator,
                    key=i,
                    function=ycsb.Read,
                    params=(i,)
                )
            )

            if len(tasks) == self.batch_size:
                async_request_responses += await asyncio.gather(*tasks)
                tasks = []

        if len(tasks) > 0:
            async_request_responses += await asyncio.gather(*tasks)

        for request in async_request_responses:
            request_id, timestamp = request
            self.requests += [{
                'run_number': self.run_number,
                'request_id': request_id,
                'function': 'Read',
                'stage': 'validation',
                'timestamp': timestamp
            }]

    async def cleanup_run(self):
        # Not needed
        pass

    async def run(self):
        self.consumer: BenchmarkConsumer = BenchmarkConsumer()
        asyncio.create_task(self.consumer.run())
        await self.consumer.consumer_ready_event.wait()

        self.universalis = Universalis(
            self.UNIVERSALIS_HOST,
            self.UNIVERSALIS_PORT,
            ingress_type=IngressTypes.KAFKA,
            kafka_url=self.KAFKA_URL
        )

        await self.universalis.submit(g, (workloads,))
        await asyncio.sleep(2)

        for run_number in range(self.num_runs):
            logging.info(f'Initialising run {run_number}...')
            await self.initialise_run(run_number)
            logging.info('Initialised')
            await asyncio.sleep(1)

            logging.info('Inserting records...')
            await self.insert_records()
            logging.info('Finished inserting')
            await asyncio.sleep(2)

            logging.info('Running transaction mix...')
            await self.run_transaction_mix()
            logging.info('Finished running transaction mix')
            await asyncio.sleep(2)

            logging.info('Running validation...')
            await self.run_validation()
            logging.info('Finished validation')
            await asyncio.sleep(2)

            logging.info('Cleaning up...')
            await self.cleanup_run()
            logging.info('Cleaned up')
            await asyncio.sleep(1)
            self.responses += await self.consumer.get_run_responses()

            logging.info(f'Run {run_number} completed')

        await self.consumer.stop()
        await self.universalis.close()
        calculate_metrics.calculate(self.requests, self.responses, self.balances, self.params)
