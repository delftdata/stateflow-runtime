from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.operator import Operator
from universalis.common.stateflow_graph import StateflowGraph

from demo.functions import order, stock, user

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('shopping-cart', operator_state_backend=LocalStateBackend.REDIS)

user_operator = Operator('user', n_partitions=6)
stock_operator = Operator('stock', n_partitions=6)
order_operator = Operator('order', n_partitions=6)
####################################################################################################################
user_operator.register_stateful_functions(user.CreateUser, user.AddCredit, user.SubtractCredit)
g.add_operator(user_operator)
####################################################################################################################
stock_operator.register_stateful_functions(stock.CreateItem, stock.AddStock, stock.SubtractStock)
g.add_operator(stock_operator)
####################################################################################################################
order_operator.register_stateful_functions(order.CreateOrder, order.AddItem, order.Checkout)
g.add_operator(order_operator)
####################################################################################################################
