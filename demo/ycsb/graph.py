from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.operator import Operator
from universalis.common.stateflow_graph import StateflowGraph

from functions import ycsb

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('ycsb_experiment', operator_state_backend=LocalStateBackend.REDIS)
ycsb_operator = Operator('ycsb', n_partitions=2)
####################################################################################################################
ycsb_operator.register_stateful_functions(ycsb.Insert, ycsb.Read, ycsb.Update, ycsb.Transfer)
g.add_operator(ycsb_operator)
