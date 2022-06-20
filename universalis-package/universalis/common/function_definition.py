from typing import Type, Union

from .networking import NetworkingManager
from .base_state import BaseOperatorState as State
from .stateful_function import StatefulFunction
from .function import Function


class FunctionDefinition:

    def __init__(self, function_definition: Type, operator_name: str):
        self.function_definition = function_definition
        self.operator_name = operator_name

    def materialize_function(self,
                             operator_state: State,
                             networking: NetworkingManager,
                             timestamp: int,
                             dns: dict[str, dict[str, tuple[str, int]]],
                             t_id: int,
                             request_id: str,
                             operator_functions: dict[str, str]) -> Union[Function, StatefulFunction]:
        return self.function_definition(self.operator_name,
                                        operator_state,
                                        networking,
                                        timestamp,
                                        dns,
                                        t_id,
                                        request_id,
                                        operator_functions)
