from typing import Union, Awaitable
from copy import copy

from universalis.common.serialization import Serializer

from .logging import logging
from .base_operator import BaseOperator
from .function import Function
from .stateful_function import StatefulFunction

SERVER_PORT = 8888


class NotAFunctionError(Exception):
    pass


class Operator(BaseOperator):

    def __init__(self,
                 name: str,
                 n_partitions: int = 1):
        super().__init__(name, n_partitions)
        self.state = None
        self.networking = None
        self.functions: dict[str, Union[Function, StatefulFunction]] = {}
        self.dns: dict[str, dict[str, tuple[str, int]]] = {}  # where the other functions exist

    async def run_function(self,
                           t_id: int,
                           request_id: str,
                           timestamp: int,
                           function_name: str,
                           ack_payload: tuple[str, int, str],
                           *params) -> Awaitable:
        function_copy = copy(self.functions[function_name])
        function_copy.set_copy(self.state, self.networking, timestamp, self.dns, t_id, request_id)
        logging.warning(f'PROCESSING FUNCTION -> {function_name}:{t_id} of operator: {self.name} with params: {params}')
        if ack_payload is not None:
            # part of a chain (not root)
            ack_host, ack_id, fraction_str = ack_payload
            resp = await function_copy(*params, ack_share=fraction_str)
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
            resp = await function_copy(*params)
            if not isinstance(resp, Exception):
                resp, _ = resp
        del function_copy
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
            function.set_operator_name(self.name)
            self.register_stateful_function(function)
