from typing import Union, Awaitable
from copy import copy

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
                 n_partitions: int = 1,
                 operator_state_backend: LocalStateBackend = LocalStateBackend.DICT):
        super().__init__(name, n_partitions)
        self.state = None
        self.networking = None
        self.operator_state_backend: LocalStateBackend = operator_state_backend
        self.functions: dict[str, Union[Function, StatefulFunction]] = {}
        self.dns: dict[str, dict[str, tuple[str, int]]] = {}  # where the other functions exist

    async def run_function(self, t_id: int, timestamp: int, function_name: str, *params) -> Awaitable:
        function_copy = copy(self.functions[function_name])
        function_copy.attach_state(self.state)
        function_copy.attach_networking(self.networking)
        function_copy.set_dns(self.dns)
        function_copy.set_timestamp(timestamp)
        function_copy.set_t_id(t_id)
        resp = await function_copy(*params)
        del function_copy
        logging.warning(f'PROCESSING FUNCTION -> {function_name} of operator: {self.name} with params: {params}')
        return resp

    def register_function(self, function: Function):
        self.functions[function.name] = function

    def register_stateful_function(self, function: StatefulFunction):
        if StatefulFunction not in type(function).__bases__:
            raise NotAFunctionError
        stateful_function = function
        self.functions[stateful_function.name] = stateful_function

    def attach_state_networking(self, state, networking, dns):
        self.state = state
        self.networking = networking
        self.dns = dns

    def register_stateful_functions(self, *functions: StatefulFunction):
        for function in functions:
            self.register_stateful_function(function)
