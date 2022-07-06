import asyncio
import random
import time
import pandas as pd

import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis

from functions import ycsb
from graph import ycsb_operator, g
from zipfian_generator import ZipfGenerator


N_ROWS = 10000

keys: list[int] = list(range(N_ROWS))
operations = ['read', 'update', 'transfer']
operation_mix = [0.5, 0.3, 0.2]

UNIVERSALIS_HOST: str = 'localhost'
UNIVERSALIS_PORT: int = 8886
KAFKA_URL = 'localhost:9093'

keys: list[int] = list(range(N_ROWS))


async def main():
    universalis = Universalis(UNIVERSALIS_HOST, UNIVERSALIS_PORT,
                              ingress_type=IngressTypes.KAFKA,
                              kafka_url=KAFKA_URL)
    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    await universalis.submit(g)
    print('Graph submitted')

    zipf_gen = ZipfGenerator(items=N_ROWS)
    timestamped_request_ids = {}
    time.sleep(1)

    print('Inserting')
    # INSERT
    tasks = []
    for i in keys:
        tasks.append(universalis.send_kafka_event(operator=ycsb_operator,
                                                  key=i,
                                                  function=ycsb.Insert,
                                                  params=(str(i),)))
    await asyncio.gather(*tasks)

    print(f'All {N_ROWS} Records Inserted')
    time.sleep(0.5)

    print('Running Transaction Mix')
    tasks = []

    for i in range(N_ROWS):
        key = keys[next(zipf_gen)]
        op = random.choice(operations)

        if op == 'read':
            tasks.append(universalis.send_kafka_event(ycsb_operator, key, ycsb.Read, (str(key),)))
        elif op == 'update':
            tasks.append(universalis.send_kafka_event(ycsb_operator, key, ycsb.Update, (str(key),)))
        else:
            key_b = keys[next(zipf_gen)]

            while key_b == key:
                key_b = keys[next(zipf_gen)]

            tasks.append(universalis.send_kafka_event(ycsb_operator, key, ycsb.Transfer, (str(key), str(key_b),)))

    responses = await asyncio.gather(*tasks)

    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    print('Transaction Mix Complete')
    time.sleep(0.5)

    await universalis.close()

    pd.DataFrame(timestamped_request_ids.items(), columns=['request_id', 'timestamp']).to_csv(
        '../requests.csv',
        index=False)

if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
