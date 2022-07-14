from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.operator import Operator
from universalis.common.stateflow_graph import StateflowGraph

from workloads.tpcc.functions import item

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################

g = StateflowGraph('tpcc_benchmark', operator_state_backend=LocalStateBackend.REDIS)
item_operator = Operator('items', n_partitions=2)
####################################################################################################################
item_operator.register_stateful_functions(item.InitialiseItems)
g.add_operator(item_operator)
