import asyncio
import configparser
import json
import random

from common.concurrency import gather_with_concurrency_limit
from common.workload import Workload
from workloads.ycsb import results
from workloads.ycsb.functions import ycsb
from workloads.ycsb.functions.graph import ycsb_operator, g
from workloads.ycsb.util.zipfian_generator import ZipfGenerator


class YcsbWorkload(Workload):
    operations: list[str] = ['Read', 'Update', 'Transfer']

    def __init__(self):
        super().__init__()
        self.graph = g
        self.num_rows: int = self.params['num_rows']
        self.batch_size: int = self.params['batch_size']
        self.batch_wait_time: float = self.params['batch_wait_time']
        self.num_operations: int = self.params['num_operations']
        self.keys: list[int] = list(range(self.num_rows))
        self.balances: dict[(int, str), dict[str, int]] = {}

    @staticmethod
    def parse_benchmark_parameters():
        config = configparser.ConfigParser()
        config.read('workload.ini')

        return {
            'workload': str(config['Benchmark']['workload']),
            'num_runs': int(config['Benchmark']['num_runs']),
            'num_rows': int(config['Benchmark']['num_rows']),
            'num_operations': int(config['Benchmark']['num_operations']),
            'num_concurrent_tasks': int(config['Benchmark']['num_concurrent_tasks']),
            'batch_size': int(config['Benchmark']['batch_size']),
            'batch_wait_time': float(config['Benchmark']['batch_wait_time']),
            'operation_mix': json.loads(config['Benchmark']['operation_mix'])
        }

    async def init_run(self, run_number):
        self.run_number = run_number

        for key in self.keys:
            self.balances[(self.run_number, str(key))] = {'expected': 100, 'received': 0}

    async def insert_records(self):
        async_request_responses = []
        requests_meta: dict[int, (str, tuple)] = {}

        tasks = []
        for i in self.keys:
            params: tuple = (i,)

            tasks.append(
                self.universalis.send_kafka_event(
                    operator=ycsb_operator,
                    key=i,
                    function=ycsb.Insert,
                    params=params
                )
            )

            requests_meta[i] = ('Insert', params)

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
                'function': requests_meta[i][0],
                'params': requests_meta[i][1],
                'stage': 'insertion',
                'timestamp': timestamp
            }]

    async def run_transaction_mix(self):
        zipf_gen = ZipfGenerator(items=self.num_rows)
        tasks = []
        async_request_responses = []
        requests_meta: dict[int, (str, tuple)] = {}

        for i in range(self.num_operations):
            key = self.keys[next(zipf_gen)]
            op = random.choices(self.operations, weights=self.operation_mix, k=1)[0]
            params: tuple

            if op == 'Transfer':
                key2 = self.keys[next(zipf_gen)]
                while key2 == key:
                    key2 = self.keys[next(zipf_gen)]

                self.balances[(self.run_number, str(key))]['expected'] -= 1
                self.balances[(self.run_number, str(key2))]['expected'] += 1

                params = (key, key2)
                tasks.append(self.universalis.send_kafka_event(ycsb_operator, key, op, params))
            elif op == 'Update':
                self.balances[(self.run_number, str(key))]['expected'] += 1
                params = (key,)
                tasks.append(self.universalis.send_kafka_event(ycsb_operator, key, op, params))
            else:
                params = (key,)
                tasks.append(self.universalis.send_kafka_event(ycsb_operator, key, op, params))

            requests_meta[i] = (op, params)

            if len(tasks) == self.batch_size:
                async_request_responses += await gather_with_concurrency_limit(self.num_concurrent_tasks, *tasks)
                await asyncio.sleep(self.batch_wait_time)
                tasks = []

        if len(tasks) > 0:
            async_request_responses += await gather_with_concurrency_limit(self.num_concurrent_tasks, *tasks)

        for i, request in enumerate(async_request_responses):
            request_id, timestamp = request
            self.requests += [{
                'run_number': self.run_number,
                'request_id': request_id,
                'function': requests_meta[i][0],
                'params': requests_meta[i][1],
                'stage': 'transaction_mix',
                'timestamp': timestamp
            }]

    async def run_validation(self):
        tasks = []
        async_request_responses = []
        requests_meta: dict[int, (str, tuple)] = {}

        for i in self.keys:
            params: tuple = (i,)

            tasks.append(
                self.universalis.send_kafka_event(
                    operator=ycsb_operator,
                    key=i,
                    function=ycsb.Read,
                    params=params
                )
            )

            requests_meta[i] = ('Read', params)

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
                'function': requests_meta[i][0],
                'params': requests_meta[i][1],
                'stage': 'validation',
                'timestamp': timestamp
            }]

    def generate_metrics(self):
        results.calculate(self.requests, self.responses, self.balances, self.params)
