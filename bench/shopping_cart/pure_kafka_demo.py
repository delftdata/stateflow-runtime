import asyncio
import time
import pandas as pd

import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis

from bench.functions import order, stock, user, graph
from bench.functions.graph import user_operator, stock_operator, order_operator

N_USERS = 5000
USER_STARTING_CREDIT = 1000
N_ITEMS = 5000
ITEM_DEFAULT_PRICE = 1
ITEM_STARTING_STOCK = 1000
N_ORDERS = 5000

UNIVERSALIS_HOST: str = 'localhost'
UNIVERSALIS_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


async def main():
    universalis = Universalis(UNIVERSALIS_HOST, UNIVERSALIS_PORT,
                              ingress_type=IngressTypes.KAFKA,
                              kafka_url=KAFKA_URL)
    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    await universalis.submit(graph.g)

    print('Graph submitted')

    timestamped_request_ids = {}

    time.sleep(1)

    # CREATE USERS
    tasks = []
    for i in range(N_USERS):
        tasks.append(universalis.send_kafka_event(operator=user_operator,
                                                  key=i,
                                                  function=user.CreateUser,
                                                  params=(i, str(i))))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    tasks = []
    for i in range(N_USERS):
        tasks.append(universalis.send_kafka_event(user_operator, i, user.AddCredit, (i, USER_STARTING_CREDIT)))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    print('Users done')
    # CREATE ITEMS
    tasks = []
    for i in range(N_ITEMS):
        tasks.append(universalis.send_kafka_event(stock_operator, i, stock.CreateItem, (i, str(i), ITEM_DEFAULT_PRICE)))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    tasks = []
    for i in range(N_ITEMS):
        tasks.append(universalis.send_kafka_event(stock_operator, i, stock.AddStock, (i, ITEM_STARTING_STOCK)))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp
    print('Items done')
    # CREATE ORDERS
    tasks = []
    for i in range(N_ORDERS):
        tasks.append(universalis.send_kafka_event(order_operator, i, order.CreateOrder, (i, i)))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    tasks = []
    for i in range(N_ORDERS):
        order_key = item_key = i
        quantity, cost = 1, 1
        tasks.append(universalis.send_kafka_event(order_operator, order_key, order.AddItem, (order_key, item_key,
                                                                                             quantity, cost)))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    tasks = []
    for i in range(N_ORDERS):
        order_key = i
        tasks.append(universalis.send_kafka_event(order_operator, order_key, order.Checkout, (order_key,)))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp
    await universalis.close()

    pd.DataFrame(timestamped_request_ids.items(), columns=['request_id', 'timestamp']).to_csv(
        'rhea-50ms-req.csv',
        index=False)

if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
