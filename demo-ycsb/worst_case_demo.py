import asyncio
import time
import pandas as pd

import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis

import ycsb
from graph import ycsb_operator, g


N_ROWS = 1000

UNIVERSALIS_HOST: str = 'localhost'
UNIVERSALIS_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


keys: list[int] = [0]


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
    for _ in range(N_ROWS):
        tasks.append(universalis.send_kafka_event(ycsb_operator, keys[0], ycsb.Update, (keys[0], )))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    await universalis.close()

    pd.DataFrame(timestamped_request_ids.items(), columns=['request_id', 'timestamp']).to_csv(
        '../demo/client_requests.csv',
        index=False)

if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
