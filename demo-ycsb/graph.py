from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.operator import Operator
from universalis.common.stateflow_graph import StateflowGraph

import ycsb

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('ycsb-experiment', operator_state_backend=LocalStateBackend.REDIS)
ycsb_operator = Operator('ycsb', n_partitions=6)
####################################################################################################################
ycsb_operator.register_stateful_functions(ycsb.Insert, ycsb.Read, ycsb.Update, ycsb.Transfer, ycsb.Debug)
g.add_operator(ycsb_operator)
