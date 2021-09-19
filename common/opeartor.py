from typing import Union

from common.function import Function
from common.state import OperatorState
from common.stateful_function import StatefulFunction


class NotAFunctionError(Exception):
    pass


class Operator:
    # each operator is a coroutine? and a server. Client is already implemented in the networking module
    def __init__(self, name: str):
        self.name = name
        self.state: OperatorState = OperatorState()
        self.functions: dict[str, Union[Function, StatefulFunction]] = {}
        self.dns = {}  # where the other functions exist

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
        self.dns = dns

    def call_remote_function(self):
        pass
