import asyncio

import uvloop

from tpcc.tpcc_benchmark import TpccBenchmark
from common.benchmark_consumer import BenchmarkConsumer
from common.calculate_metrics import calculate
from ycsb.ycsb_benchmark import YcsbBenchmark

tpcc = TpccBenchmark()
ycsb = YcsbBenchmark()


async def run():
    tasks = [bench.run(), consumer.run()]
    await asyncio.gather(*tasks)
    calculate()

if __name__ == "__main__":
    uvloop.install()
    bench = tpcc
    consumer = BenchmarkConsumer()
    asyncio.run(run())
