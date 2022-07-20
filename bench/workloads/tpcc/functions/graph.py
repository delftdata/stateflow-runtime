from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.operator import Operator
from universalis.common.stateflow_graph import StateflowGraph

import workloads.tpcc.functions.customer as customer
import workloads.tpcc.functions.district as district
import workloads.tpcc.functions.history as history
import workloads.tpcc.functions.item as item
import workloads.tpcc.functions.new_order as new_order
import workloads.tpcc.functions.order as order
import workloads.tpcc.functions.order_line as order_line
import workloads.tpcc.functions.stock as stock
import workloads.tpcc.functions.warehouse as warehouse

PARTITIONS_PER_OPERATOR: int = 2

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('tpcc_benchmark', operator_state_backend=LocalStateBackend.REDIS)

####################################################################################################################
# DECLARE OPERATORS ########################################################################################
####################################################################################################################
customer_operator = Operator('customer', n_partitions=PARTITIONS_PER_OPERATOR)
district_operator = Operator('district', n_partitions=PARTITIONS_PER_OPERATOR)
history_operator = Operator('history', n_partitions=PARTITIONS_PER_OPERATOR)
item_operator = Operator('item', n_partitions=PARTITIONS_PER_OPERATOR)
new_order_operator = Operator('new_order', n_partitions=PARTITIONS_PER_OPERATOR)
order_operator = Operator('order', n_partitions=PARTITIONS_PER_OPERATOR)
order_line_operator = Operator('order_line', n_partitions=PARTITIONS_PER_OPERATOR)
stock_operator = Operator('stock', n_partitions=PARTITIONS_PER_OPERATOR)
warehouse_operator = Operator('warehouse', n_partitions=PARTITIONS_PER_OPERATOR)

####################################################################################################################
# REGISTER OPERATOR FUNCTIONS ########################################################################################
####################################################################################################################
customer_operator.register_stateful_functions(customer.Insert, customer.Payment)
district_operator.register_stateful_functions(district.Insert, district.Get)
history_operator.register_stateful_functions(history.Insert, history.Get)
item_operator.register_stateful_functions(item.Insert, item.Get)
new_order_operator.register_stateful_functions(new_order.Insert, new_order.Get)
order_operator.register_stateful_functions(order.Insert, order.Get, order.NewOrder)
order_line_operator.register_stateful_functions(order_line.Insert, order_line.Get)
stock_operator.register_stateful_functions(stock.Insert, stock.Get)
warehouse_operator.register_stateful_functions(warehouse.Insert, warehouse.Get)

g.add_operator(customer_operator)
g.add_operator(district_operator)
g.add_operator(history_operator)
g.add_operator(item_operator)
g.add_operator(new_order_operator)
g.add_operator(order_operator)
g.add_operator(order_line_operator)
g.add_operator(stock_operator)
g.add_operator(warehouse_operator)
