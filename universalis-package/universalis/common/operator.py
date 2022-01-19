from typing import Union, Awaitable

from .logging import logging
from .base_operator import BaseOperator
from .function import Function
from .stateful_function import StatefulFunction
from .local_state_backends import LocalStateBackend


class NotAFunctionError(Exception):
    pass


class Operator(BaseOperator):

    def __init__(self,
                 name: str,
                 partitions: int = 1,
                 operator_state_backend: LocalStateBackend = LocalStateBackend.DICT):
        super().__init__(name)
        self.state = None
        self.networking = None
        self.operator_state_backend: LocalStateBackend = operator_state_backend
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

    def attach_state_to_functions(self, state, networking):
        self.state = state
        self.networking = networking
        for function in self.functions.values():
            function.attach_state(self.state)
            function.attach_networking(self.networking)

    def register_stateful_functions(self, *functions: StatefulFunction):
        for function in functions:
            self.register_stateful_function(function)

    def set_function_timestamp(self, fun_name: str, timestamp: int):
        self.functions[fun_name].set_timestamp(timestamp)

    def set_function_dns(self, fun_name: str, dns):
        self.dns = dns
        self.functions[fun_name].set_dns(self.dns)
