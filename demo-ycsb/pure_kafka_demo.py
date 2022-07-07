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
    operations = ["r", "u", "t"]
    operation_mix = [0.5, 0.3, 0.2]
    operation_counts: dict[str, int] = {"r": 0, "u": 0, "t": 0}

    # INSERT
    tasks = []
    for i in keys:
        tasks.append(universalis.send_kafka_event(operator=ycsb_operator,
                                                  key=i,
                                                  function=ycsb.Insert,
                                                  params=(i, )))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    time.sleep(1)

    tasks = []
    for i in range(N_ROWS):
        key = keys[next(zipf_gen)]
        op = random.choices(operations, weights=operation_mix, k=1)[0]
        operation_counts[op] += 1
        if op == "r":
            tasks.append(universalis.send_kafka_event(ycsb_operator, key, ycsb.Read, (key, )))
        elif op == "u":
            tasks.append(universalis.send_kafka_event(ycsb_operator, key, ycsb.Update, (key, )))
        else:
            key2 = keys[next(zipf_gen)]
            while key2 == key:
                key2 = keys[next(zipf_gen)]
            tasks.append(universalis.send_kafka_event(ycsb_operator, key, ycsb.Transfer, (key, key2)))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    print(operation_counts)

    await universalis.close()

    pd.DataFrame(timestamped_request_ids.items(), columns=['request_id', 'timestamp']).to_csv(
        '../demo/client_requests.csv',
        index=False)

if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
