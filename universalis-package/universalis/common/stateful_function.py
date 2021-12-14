import os
import uuid
from abc import abstractmethod

from universalis.common.logging import logging
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
    request_response_store: State
    networking: NetworkingManager

    def __init__(self):
        super().__init__()
        self.dns: dict[dict[str, tuple[str, int]]] = {}
        self.timestamp = None
        self.remote_calls = []

    async def __call__(self, *args, **kwargs):
        if self.state is None:
            raise StateNotAttachedError('Cannot call stateful function without attached state')
        try:
            return await self.run(*args)
        except TypeError:
            pass

    def call_remote_function_no_response(self, operator_name, function_name, key, params):
        # logging.warning(f"DNS: {self.dns}")
        if operator_name not in self.dns:
            logging.warning(f"Couldn't find operator: {operator_name}")
            self.__call_discovery()

        partition: str = str(int(make_key_hashable(key)) % len(self.dns[operator_name].keys()))

        payload = {'__OP_NAME__': operator_name,
                   '__FUN_NAME__': function_name,
                   '__KEY__': key,
                   '__PARTITION__': int(partition),
                   '__TIMESTAMP__': self.timestamp,
                   '__PARAMS__': params}

        operator_host, operator_port = self.dns[operator_name][partition][0], self.dns[operator_name][partition][1]

        self.networking.send_message(operator_host,
                                     operator_port,
                                     operator_name,
                                     function_name,
                                     {"__COM_TYPE__": 'RUN_FUN', "__MSG__": payload},
                                     Serializer.MSGPACK)

    def call_remote_function_request_response(self, operator_name, function_name, key, params):
        # logging.warning(f"DNS: {self.dns}")
        if operator_name not in self.dns:
            logging.warning(f"Couldn't find operator: {operator_name} in {self.dns}")
            self.__call_discovery()

        partition: str = str(int(make_key_hashable(key)) % len(self.dns[operator_name].keys()))

        payload = {'__OP_NAME__': operator_name,
                   '__FUN_NAME__': function_name,
                   '__KEY__': key,
                   '__PARTITION__': int(partition),
                   '__TIMESTAMP__': self.timestamp,
                   '__PARAMS__': params}

        operator_host, operator_port = self.dns[operator_name][partition][0], self.dns[operator_name][partition][1]
        logging.warning(f'(SF)  Start {operator_host}:{operator_port} of {operator_name}:{partition}')

        resp = self.networking.send_message_request_response(operator_host,
                                                             operator_port,
                                                             operator_name,
                                                             function_name,
                                                             {"__COM_TYPE__": 'RUN_FUN_RQ_RS', "__MSG__": payload},
                                                             Serializer.MSGPACK)
        return resp

    def attach_state(self, operator_state: State, request_response_store: State):
        self.state = operator_state
        self.request_response_store = request_response_store

    def attach_networking(self, networking):
        self.networking = networking

    def set_timestamp(self, timestamp: int):
        self.timestamp = timestamp

    def set_dns(self, dns):
        self.dns = dns
        # logging.warning(f"SETTING DNS TO: {self.dns}")

    def __call_discovery(self):
        discovery_host, discovery_port = os.environ['DISCOVERY_HOST'], int(os.environ['DISCOVERY_PORT'])
        self.networking.send_message(discovery_host,
                                     discovery_port,
                                     "",
                                     ({"__COM_TYPE__": "DISCOVER", "__MSG__": ""}),
                                     Serializer.MSGPACK)
        # logging.warning(f'(SF) DISC')
        self.dns = self.networking.receive_message(discovery_host, discovery_port, "")

    @abstractmethod
    async def run(self, *args):
        raise NotImplementedError
