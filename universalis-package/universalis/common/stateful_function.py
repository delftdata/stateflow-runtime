import os
import uuid
from abc import abstractmethod

from universalis.common.networking import NetworkingManager

from .serialization import Serializer
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
        self.networking = NetworkingManager()
        self.timestamp = None

    async def __call__(self, *args, **kwargs):
        if self.state is None:
            raise StateNotAttachedError('Cannot call stateful function without attached state')
        try:
            return await self.run(*args)
        except TypeError:
            pass

    def call_remote_function_no_response(self, operator_name, function_name, key, params):

        discovery_host, discovery_port = os.environ['DISCOVERY_HOST'], int(os.environ['DISCOVERY_PORT'])

        if (discovery_host, discovery_port) not in self.networking.open_socket_connections:
            self.networking.create_socket_connection(discovery_host, discovery_port)

        if operator_name not in self.dns:
            self.networking.send_message(discovery_host,
                                         discovery_port,
                                         ({"__COM_TYPE__": "DISCOVER", "__MSG__": ""}),
                                         Serializer.MSGPACK)
            self.dns = self.networking.receive_message(discovery_host, discovery_port)

        partition: str = str(int(make_key_hashable(key)) % len(self.dns[operator_name].keys()))

        payload = {'__OP_NAME__': operator_name,
                   '__FUN_NAME__': function_name,
                   '__PARTITION__': int(partition),
                   '__TIMESTAMP__': self.timestamp,
                   '__PARAMS__': params}

        operator_host, operator_port = self.dns[operator_name][partition][0], self.dns[operator_name][partition][1]

        if (operator_host, operator_port) not in self.networking.open_socket_connections:
            self.networking.create_socket_connection(operator_host,
                                                     operator_port)
        self.networking.send_message(operator_host,
                                     operator_port,
                                     {"__COM_TYPE__": 'RUN_FUN', "__MSG__": payload},
                                     Serializer.MSGPACK)

    def attach_state(self, operator_state: State):
        self.state = operator_state

    # def attach_networking(self, networking):
    #     self.networking = networking

    def set_timestamp(self, timestamp: int):
        self.timestamp = timestamp

    @abstractmethod
    async def run(self, *args):
        raise NotImplementedError
