import asyncio
import random
import time
import pandas as pd

import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis

import ycsb
from graph import ycsb_operator, g
from zipfian_generator import ZipfGenerator


N_ROWS = 1000

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

    timestamped_request_ids = {}

    time.sleep(1)

    zipf_gen = ZipfGenerator(items=N_ROWS)
    operations = ["r", "u"]

    # INSERT
    tasks = []
    for i in keys:
        tasks.append(universalis.send_kafka_event(operator=ycsb_operator,
                                                  key=i,
                                                  function=ycsb.Insert,
                                                  params=(i, str(i))))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    time.sleep(1)

    tasks = []
    for i in range(N_ROWS):
        key = keys[next(zipf_gen)]
        op = random.choice(operations)
        if op == "r":
            tasks.append(universalis.send_kafka_event(ycsb_operator, key, ycsb.Read, (key, )))
        else:
            tasks.append(universalis.send_kafka_event(ycsb_operator, key, ycsb.Update, (key, str(key))))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    await universalis.close()

    pd.DataFrame(timestamped_request_ids.items(), columns=['request_id', 'timestamp']).to_csv(
        '../demo/client_requests.csv',
        index=False)

uvloop.install()
asyncio.run(main())
