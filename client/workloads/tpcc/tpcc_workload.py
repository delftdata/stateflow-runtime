import configparser
import json

from common.workload import Workload
from workloads.tpcc import results
from workloads.tpcc.functions.graph import g
from workloads.tpcc.runtime.executor import Executor
from workloads.tpcc.runtime.loader import Loader
from workloads.tpcc.util import rand, nurand
from workloads.tpcc.util.scale_parameters import make_with_scale_factor


class TpccWorkload(Workload):
    def __init__(self):
        super().__init__()
        self.graph = g
        self.scale_parameters = make_with_scale_factor(self.params['num_warehouses'], self.params['scale_factor'])
        self.nu_rand = rand.set_nu_rand(nurand.make_for_load())

    async def init_run(self, run_number):
        self.run_number = run_number

    @staticmethod
    def parse_benchmark_parameters() -> dict:
        config = configparser.ConfigParser()
        config.read('workload.ini')

        return {
            'workload': str(config['Benchmark']['workload']),
            'num_runs': int(config['Benchmark']['num_runs']),
            'num_concurrent_tasks': int(config['Benchmark']['num_concurrent_tasks']),
            'operation_mix': json.loads(config['Benchmark']['operation_mix']),
            'loader_batch_size': int(config['Benchmark']['loader_batch_size']),
            'loader_batch_wait_time': float(config['Benchmark']['loader_batch_wait_time']),
            'executor_batch_size': int(config['Benchmark']['executor_batch_size']),
            'executor_batch_wait_time': float(config['Benchmark']['executor_batch_wait_time']),
            'benchmark_duration': int(config['Benchmark']['benchmark_duration']),
            'scale_factor': int(config['Benchmark']['scale_factor']),
            'num_warehouses': int(config['Benchmark']['num_warehouses']),
        }

    async def insert_records(self):
        loader = Loader(
            self.params,
            self.scale_parameters,
            list(range(1, self.scale_parameters.warehouses + 1)),
            self.universalis
        )
        self.requests += await loader.execute(self.run_number)

    async def run_transaction_mix(self):
        executor = Executor(self.params, self.scale_parameters, self.universalis)
        self.requests += await executor.execute_transactions(self.run_number)

    async def run_validation(self):
        # No validation for TPCC_throughput yet
        pass

    def generate_metrics(self):
        results.calculate(self.requests, self.responses, self.params)
