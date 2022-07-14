import asyncio

import uvloop

from common.calculate_metrics import calculate
from consumer.consumer import BenchmarkConsumer
from workloads.tpcc.tpcc_benchmark import TpccBenchmark
from workloads.ycsb.ycsb_benchmark import YcsbBenchmark

tpcc = TpccBenchmark()
ycsb = YcsbBenchmark()


async def main():
    tasks = [bench.run(), consumer.main()]
    await asyncio.gather(*tasks)
    calculate()


if __name__ == "__main__":
    uvloop.install()
    bench = tpcc
    consumer = BenchmarkConsumer()
    asyncio.run(main())
