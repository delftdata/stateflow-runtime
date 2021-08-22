from coordinator.coordinator import Coordinator
from coordinator.stateflow_graph import StateflowGraph
from demo.order import CreateOrder, AddItem, Checkout
from demo.stock import CreateItem, AddStock, SubtractStock
from demo.user import CreateUser, AddCredit, SubtractCredit
from universalis_operator.opeartor import Operator

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
####################################################################################################################
####################################################################################################################

coordinator = Coordinator("0.0.0.0", 8886)

coordinator.submit_stateflow_graph(g)
