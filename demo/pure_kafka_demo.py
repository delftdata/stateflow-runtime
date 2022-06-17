import asyncio
import time

import uvloop
from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.operator import Operator
from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis

from demo.functions import order, stock, user

N_USERS = 1000
USER_STARTING_CREDIT = 1000
N_ITEMS = 1000
ITEM_DEFAULT_PRICE = 1
ITEM_STARTING_STOCK = 1000
N_ORDERS = 1000

UNIVERSALIS_HOST: str = 'localhost'
UNIVERSALIS_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


async def main():
    universalis = Universalis(UNIVERSALIS_HOST, UNIVERSALIS_PORT,
                              ingress_type=IngressTypes.KAFKA,
                              kafka_url=KAFKA_URL)

    user_operator = Operator('user', n_partitions=6)
    stock_operator = Operator('stock', n_partitions=6)
    order_operator = Operator('order', n_partitions=6)

    ####################################################################################################################
    # DECLARE A STATEFLOW GRAPH ########################################################################################
    ####################################################################################################################
    g = StateflowGraph('shopping-cart', operator_state_backend=LocalStateBackend.REDIS)
    ####################################################################################################################
    user_operator.register_stateful_functions(user.CreateUser(), user.AddCredit(), user.SubtractCredit())
    g.add_operator(user_operator)
    ####################################################################################################################
    stock_operator.register_stateful_functions(stock.CreateItem(), stock.AddStock(), stock.SubtractStock())
    g.add_operator(stock_operator)
    ####################################################################################################################
    order_operator.register_stateful_functions(order.CreateOrder(), order.AddItem(), order.Checkout())
    g.add_operator(order_operator)
    ####################################################################################################################
    g.add_connection(order_operator, user_operator, bidirectional=True)
    g.add_connection(order_operator, stock_operator, bidirectional=True)
    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    await universalis.submit(g, user, order, stock)

    print('Graph submitted')

    time.sleep(2)

    # CREATE USERS

    for i in range(N_USERS):
        await universalis.send_kafka_event(operator=user_operator,
                                           key=i,
                                           function=user.CreateUser(),
                                           params=(i, str(i)))

    time.sleep(2)

    for i in range(N_USERS):
        await universalis.send_kafka_event(user_operator, i, user.AddCredit(), (i, USER_STARTING_CREDIT))

    time.sleep(2)

    # CREATE ITEMS

    for i in range(N_ITEMS):
        await universalis.send_kafka_event(stock_operator, i, stock.CreateItem(), (i, str(i), ITEM_DEFAULT_PRICE))

    time.sleep(2)

    for i in range(N_ITEMS):
        await universalis.send_kafka_event(stock_operator, i, stock.AddStock(), (i, ITEM_STARTING_STOCK))

    time.sleep(2)

    # CREATE ORDERS

    for i in range(N_ORDERS):
        await universalis.send_kafka_event(order_operator, i, order.CreateOrder(), (i, i))

    time.sleep(2)

    for i in range(N_ORDERS):
        order_key = item_key = i
        quantity, cost = 1, 1
        await universalis.send_kafka_event(order_operator, order_key, order.AddItem(), (order_key, item_key,
                                                                                        quantity, cost))

    time.sleep(2)

    for i in range(N_ORDERS):
        order_key = i
        await universalis.send_kafka_event(order_operator, order_key, order.Checkout(), (order_key,))

    time.sleep(2)
    await universalis.close()

uvloop.install()
asyncio.run(main())
