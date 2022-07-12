import asyncio

import uvloop

from common.benchmark_consumer import BenchmarkConsumer
from common.calculate_metrics import calculate
from ycsb.ycsb_benchmark import YcsbBenchmark


async def run():
    tasks = [bench.run(), consumer.run()]
    await asyncio.gather(*tasks)
    calculate()

if __name__ == "__main__":
    uvloop.install()
    bench = YcsbBenchmark()
    consumer = BenchmarkConsumer()
    asyncio.run(run())
