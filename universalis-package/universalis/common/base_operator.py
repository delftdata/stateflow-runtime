from universalis.common.function_definition import FunctionDefinition


class BaseOperator:

    def __init__(self, name: str, n_partitions: int = 1):
        self.name: str = name  # operator's name
        self.n_partitions: int = n_partitions  # number of partitions
        self.functions: dict[str, FunctionDefinition] = {}
