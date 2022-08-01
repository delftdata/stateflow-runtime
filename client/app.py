import asyncio
import configparser

import uvloop

from workloads.tpcc.tpcc_benchmark import TpccBenchmark
from workloads.ycsb.ycsb_benchmark import YcsbBenchmark

config = configparser.ConfigParser()
config.read('workload.ini')
workload: str = str(config['Benchmark']['workload'])


async def main():
    if workload == 'ycsb':
        bench = YcsbBenchmark()
    else:
        bench = TpccBenchmark()

    await bench.run()


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
