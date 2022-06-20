from typing import Awaitable, Type

from .function_definition import FunctionDefinition
from .serialization import Serializer
from .logging import logging
from .base_operator import BaseOperator
from .function import Function
from .stateful_function import StatefulFunction

SERVER_PORT = 8888


class NotAFunctionError(Exception):
    pass


class Operator(BaseOperator):

    operator_functions: dict[str, str]  # function_name: operator_name

    def __init__(self,
                 name: str,
                 n_partitions: int = 1):
        super().__init__(name, n_partitions)
        self.state = None
        self.networking = None
        self.dns: dict[str, dict[str, tuple[str, int]]] = {}  # where the other functions exist

    async def run_function(self,
                           t_id: int,
                           request_id: str,
                           timestamp: int,
                           function_name: str,
                           ack_payload: tuple[str, int, str],
                           *params) -> Awaitable:
        function = self.functions[function_name].materialize_function(self.state,
                                                                      self.networking,
                                                                      timestamp,
                                                                      self.dns,
                                                                      t_id,
                                                                      request_id,
                                                                      self.operator_functions)
        logging.info(f'PROCESSING FUNCTION -> {function_name}:{t_id} of operator: {self.name} with params: {params}')
        if ack_payload is not None:
            # part of a chain (not root)
            ack_host, ack_id, fraction_str = ack_payload
            resp = await function(*params, ack_share=fraction_str)
            if not isinstance(resp, Exception):
                resp, n_remote_calls = resp
                if n_remote_calls == 0:
                    # final link of the chain (send ack share)
                    await self.networking.send_message(ack_host, SERVER_PORT, {"__COM_TYPE__": 'ACK',
                                                                               "__MSG__": (ack_id, fraction_str)},
                                                       Serializer.MSGPACK)
            else:
                # Send chain failure
                await self.networking.send_message(ack_host, SERVER_PORT, {"__COM_TYPE__": 'ACK',
                                                                           "__MSG__": (ack_id, '-1')},
                                                   Serializer.MSGPACK)
        else:
            resp = await function(*params)
            if not isinstance(resp, Exception):
                resp, _ = resp
        del function
        return resp

    def register_function(self, function: Type):
        if Function not in function.__bases__:
            raise NotAFunctionError
        function_definition = FunctionDefinition(function, self.name)
        self.functions[function.__name__] = function_definition

    def register_stateful_function(self, function: Type):
        if StatefulFunction not in function.__bases__:
            raise NotAFunctionError
        function_definition = FunctionDefinition(function, self.name)
        self.functions[function.__name__] = function_definition

    def attach_state_networking(self, state, networking, dns, operator_functions):
        self.state = state
        self.networking = networking
        self.dns = dns
        self.operator_functions = operator_functions

    def register_stateful_functions(self, *function_definitions: Type):
        for function in function_definitions:
            self.register_stateful_function(function)
