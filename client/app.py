import asyncio
import configparser

import uvloop

from workloads.tpcc.tpcc_workload import TpccWorkload
from workloads.ycsb.ycsb_workload import YcsbWorkload

config = configparser.ConfigParser()
config.read('workload.ini')
workload: str = str(config['Benchmark']['workload'])


async def main():
    if workload == 'ycsb':
        bench = YcsbWorkload()
    else:
        bench = TpccWorkload()

    await bench.run()


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
