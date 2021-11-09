import os
import uuid
from abc import abstractmethod

import cloudpickle
from universalis.common.serialization import msgpack_serialization, msgpack_deserialization

from .networking import async_transmit_websocket_no_response_new_connection, \
    async_transmit_websocket_request_response_new_connection
from .function import Function
from .base_state import BaseOperatorState as State


class StrKeyNotUUID(Exception):
    pass


class NonSupportedKeyType(Exception):
    pass


class StateNotAttachedError(Exception):
    pass


def make_key_hashable(key):
    if isinstance(key, str):
        try:
            key = uuid.UUID(key)
        except ValueError:
            raise StrKeyNotUUID()
    elif not isinstance(key, int):
        raise NonSupportedKeyType()
    return key


class StatefulFunction(Function):

    state: State

    def __init__(self):
        super().__init__()
        self.dns: dict[dict[str, tuple[str, int]]] = {}
        self.sate = None
        self.networking = None
        self.timestamp = None

    async def __call__(self, *args, **kwargs):
        if self.state is None:
            raise StateNotAttachedError('Cannot call stateful function without attached state')
        try:
            return await self.run(*args)
        except TypeError:
            pass

    async def call_remote_function_no_response(self, operator_name, function_name, key, params):

        DISCOVERY_HOST = os.environ['DISCOVERY_HOST']
        DISCOVERY_PORT = int(os.environ['DISCOVERY_PORT'])
        if (DISCOVERY_HOST, DISCOVERY_PORT) not in self.networking.open_socket_connections:
            self.networking.create_socket_connection(DISCOVERY_HOST, DISCOVERY_PORT)

        if operator_name not in self.dns:
            # self.dns = await async_transmit_tcp_request_response(os.environ['DISCOVERY_HOST'],
            #                                                      int(os.environ['DISCOVERY_PORT']),
            #                                                      "",
            #                                                      com_type="DISCOVER")
            # self.dns = await async_transmit_websocket_request_response_new_connection(os.environ['DISCOVERY_HOST'],
            #                                                                           int(os.environ['DISCOVERY_PORT']),
            #                                                                           "",
            #                                                                           com_type="DISCOVER")
            self.networking.send_message(DISCOVERY_HOST,
                                         DISCOVERY_PORT,
                                         msgpack_serialization({"__COM_TYPE__": "DISCOVER",
                                                                "__MSG__": ""}))
            self.dns = msgpack_deserialization(self.networking.receive_message(DISCOVERY_HOST,
                                                                               DISCOVERY_PORT))

        partition: str = str(int(make_key_hashable(key)) % len(self.dns[operator_name].keys()))

        payload = {'__OP_NAME__': operator_name,
                   '__FUN_NAME__': function_name,
                   '__PARTITION__': int(partition),
                   '__TIMESTAMP__': self.timestamp,
                   '__PARAMS__': params}

        # await async_transmit_tcp_no_response(self.dns[operator_name][partition][0],
        #                                      self.dns[operator_name][partition][1],
        #                                      payload,
        #                                      com_type='RUN_FUN')
        # await async_transmit_websocket_no_response_new_connection(self.dns[operator_name][partition][0],
        #                                                           self.dns[operator_name][partition][1],
        #                                                           payload,
        #                                                           com_type='RUN_FUN')
        if (self.dns[operator_name][partition][0], self.dns[operator_name][partition][1]) not in self.networking.open_socket_connections:
            self.networking.create_socket_connection(self.dns[operator_name][partition][0],
                                                     self.dns[operator_name][partition][1])
        self.networking.send_message(self.dns[operator_name][partition][0],
                                     self.dns[operator_name][partition][1],
                                     cloudpickle.dumps({"__COM_TYPE__": 'RUN_FUN', "__MSG__": payload}))

    def attach_state(self, operator_state: State):
        self.state = operator_state

    def attach_networking(self, networking):
        self.networking = networking

    def set_timestamp(self, timestamp: int):
        self.timestamp = timestamp

    @abstractmethod
    async def run(self, *args):
        raise NotImplementedError
