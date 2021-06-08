class DataflowGraph:

    def __init__(self):
        self.edges = list()
        self.nodes = list()

    def add_edge(self, edge):
        self.edges.append(edge)

    def add_node(self, node):
        self.edges.append(node)


def generate_ones(amount: int):
    for _ in range(amount):
        yield 1


def add_one(number: int):
    return number + 1


if __name__ == "__main__":
    dg = DataflowGraph()
    dg.add_node(generate_ones(10))
    dg.add_node(add_one)
