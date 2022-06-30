from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.operator import BaseOperator


class StateflowGraph:

    def __init__(self, name: str, operator_state_backend: LocalStateBackend):
        self.name: str = name
        self.operator_state_backend: LocalStateBackend = operator_state_backend
        self.nodes: dict[str, BaseOperator] = {}

    def add_operator(self, operator: BaseOperator):
        self.nodes[operator.name] = operator

    def __iter__(self):
        return (
            (operator_name,
             self.nodes[operator_name]
             )
            for operator_name in self.nodes.keys())
