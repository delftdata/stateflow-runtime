import uuid

from flask import Flask, jsonify, Response

from common.opeartor import Operator
from common.stateflow_graph import StateflowGraph
import user
import stock
import order
from universalis.universalis import Universalis

app = Flask(__name__)


@app.post('/user/create')
def create_user():
    user_key: str = str(uuid.uuid4())
    user_name: str = f'user-{user_key}'
    universalis.send_tcp_event(user_operator, user.CreateUser(), user_key, user_name)
    return jsonify({'user_key': user_key})


@app.post('/user/add_credit/<user_key>/<amount>')
def add_credit(user_key: str, amount: int):
    universalis.send_tcp_event(user_operator, user.AddCredit(), user_key, amount)
    return Response('Credit added', status=200)


@app.post('/stock/create')
def create_item():
    item_key: str = str(uuid.uuid4())
    item_name: str = f'user-{item_key}'
    price: int = 1
    universalis.send_tcp_event(stock_operator, stock.CreateItem(), item_key, item_name, price)
    return jsonify({'item_key': item_key})


@app.post('/stock/add_stock/<item_key>/<amount>')
def add_stock(item_key: str, amount: int):
    universalis.send_tcp_event(stock_operator, stock.AddStock(), item_key, amount)
    return jsonify({'item_key': item_key})


@app.post('/order/create_order/<user_key>')
def create_order(user_key: str):
    order_key: str = str(uuid.uuid4())
    universalis.send_tcp_event(order_operator, order.CreateOrder(), order_key, user_key)
    return jsonify({'order_key': order_key})


@app.post('/order/add_item/<item_key>')
def add_item(order_key: str, item_key: str):
    quantity, cost = 1, 1
    universalis.send_tcp_event(order_operator, order.AddItem(), order_key, item_key, quantity, cost)
    return Response('Item added', status=200)


@app.post('/order/checkout/<order_key>')
def checkout_order(order_key: str):
    universalis.send_tcp_event(order_operator, order.Checkout(), order_key)
    return Response('Checkout started', status=200)


if __name__ == '__main__':
    ####################################################################################################################
    # DECLARE A STATEFLOW GRAPH ########################################################################################
    ####################################################################################################################
    g = StateflowGraph('shopping-cart')
    ####################################################################################################################

    user_operator = Operator('user')
    user_operator.register_stateful_functions(user.CreateUser(), user.AddCredit(), user.SubtractCredit())
    g.add_operator(user_operator)
    ####################################################################################################################

    stock_operator = Operator('stock')
    stock_operator.register_stateful_functions(stock.CreateItem(), stock.AddStock(), stock.SubtractStock())
    g.add_operator(stock_operator)
    ####################################################################################################################

    order_operator = Operator('order')
    order_operator.register_stateful_functions(order.CreateOrder(), order.AddItem(), order.Checkout())
    g.add_operator(order_operator)
    ####################################################################################################################

    g.add_connection(order_operator, user_operator, bidirectional=True)
    g.add_connection(order_operator, stock_operator, bidirectional=True)
    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################

    universalis = Universalis("0.0.0.0", 8886)

    universalis.submit(g, user, order, stock)
    app.run(debug=False)
