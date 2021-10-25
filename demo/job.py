from universalis.common.stateflow_graph import StateflowGraph
from universalis.universalis import Universalis
from universalis.common.operator import Operator

from demo.functions import order, stock, user

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

n_users = 1000
for usr_idx in range(n_users):
    universalis.send_tcp_event(user_operator, user.CreateUser(), usr_idx, f'user-{usr_idx}')
    universalis.send_tcp_event(user_operator, user.AddCredit(), usr_idx, 1000)
    
