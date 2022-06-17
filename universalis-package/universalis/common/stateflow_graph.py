from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.operator import BaseOperator


class StateflowGraph:

    def __init__(self, name: str, operator_state_backend: LocalStateBackend):
        self.name: str = name
        self.operator_state_backend: LocalStateBackend = operator_state_backend
        self.nodes: dict[str, BaseOperator] = {}
        self.edges: dict[str, list[str]] = {}

    def add_operator(self, operator: BaseOperator):
        self.nodes[operator.name] = operator

    def add_connection(self, operator1: BaseOperator, operator2: BaseOperator, bidirectional: bool = False):
        self.__add_edge(operator1.name, operator2.name)
        if bidirectional:
            self.__add_edge(operator2.name, operator1.name)

    def __add_edge(self, key: str, value: str):
        if key in self.edges:
            self.edges[key].append(value)
        else:
            self.edges[key] = [value]

    def __iter__(self):
        return ((operator_name, self.nodes[operator_name], self.edges[operator_name])
                for operator_name in self.nodes.keys())
