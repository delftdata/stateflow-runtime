from common.stateflow_graph import StateflowGraph
import demo
from demo.order import CreateOrder, AddItem, Checkout
from demo.stock import CreateItem, AddStock, SubtractStock
from demo.user import CreateUser, AddCredit, SubtractCredit
from universalis.universalis import Universalis
from common.opeartor import Operator

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('shopping-cart')
####################################################################################################################

user_operator = Operator('user')
user_operator.register_stateful_functions(CreateUser(), AddCredit(), SubtractCredit())
g.add_operator(user_operator)
####################################################################################################################

stock_operator = Operator('stock')
stock_operator.register_stateful_functions(CreateItem(), AddStock(), SubtractStock())
g.add_operator(stock_operator)
####################################################################################################################

order_operator = Operator('order')
order_operator.register_stateful_functions(CreateOrder(), AddItem(), Checkout())
g.add_operator(order_operator)
####################################################################################################################

g.add_connection(order_operator, user_operator, bidirectional=True)
g.add_connection(order_operator, stock_operator, bidirectional=True)
####################################################################################################################
# SUBMIT STATEFLOW GRAPH ###########################################################################################
####################################################################################################################
#
universalis = Universalis("0.0.0.0", 8886)
universalis.submit(g, demo)

n_users = 1000
for usr_idx in range(n_users):
    universalis.send_tcp_event(user_operator, CreateUser(), usr_idx, f'user-{usr_idx}')

for usr_idx in range(n_users):
    universalis.send_tcp_event(user_operator, AddCredit(), usr_idx, 1000)
