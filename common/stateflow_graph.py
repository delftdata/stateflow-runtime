from common.opeartor import Operator


class StateflowGraph:

    def __init__(self, name: str):
        self.name = name
        self.nodes = {}
        self.edges: dict[str, list[str]] = {}

    def add_operator(self, operator: Operator):
        self.nodes[operator.name] = operator

    def add_connection(self, operator1: Operator, operator2: Operator, bidirectional: bool = False):
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
