from typing import Union, Awaitable

from common.logging import logging
from common.base_operator import BaseOperator
from common.function import Function
from common.state import OperatorState
from common.stateful_function import StatefulFunction


class NotAFunctionError(Exception):
    pass


class Operator(BaseOperator):
    # each operator is a coroutine? and a server. Client is already implemented in the networking module
    def __init__(self, name: str, partitions: int = 1):
        super().__init__(name)
        self.state: OperatorState = OperatorState()
        self.functions: dict[str, Union[Function, StatefulFunction]] = {}
        self.dns = {}  # where the other functions exist
        self.partitions = partitions

    async def run_function(self, function_name: str, *params) -> Awaitable:
        logging.info(f'PROCESSING FUNCTION -> {function_name} of operator: {self.name} with params: {params}')
        return await self.functions[function_name](*params)

    def register_function(self, function: Function):
        self.functions[function.name] = function

    def register_stateful_function(self, function: StatefulFunction):
        if StatefulFunction not in type(function).__bases__:
            raise NotAFunctionError
        stateful_function = function
        self.functions[stateful_function.name] = stateful_function
        self.functions[stateful_function.name].attach_state(self.state)

    def register_stateful_functions(self, *functions: StatefulFunction):
        for function in functions:
            self.register_stateful_function(function)

    def set_dns(self, dns):
        for function in self.functions.values():
            function.set_dns(dns)
