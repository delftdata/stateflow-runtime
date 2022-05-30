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
            key = uuid.UUID(key)  # uuid type given by the user
        except ValueError:
            key = uuid.uuid5(uuid.NAMESPACE_DNS, key)  # str that we hash to SHA-1
    elif not isinstance(key, int):
        raise NonSupportedKeyType()  # if not int, str or uuid throw exception
    return key


class StatefulFunction(Function):

    state: State
    networking: NetworkingManager
    dns: dict[str, dict[str, tuple[str, int]]]
    timestamp: int  # timestamp
    t_id: int  # transaction id

    def __init__(self):
        super().__init__()

    async def __call__(self, *args, **kwargs):
        # if self.state is None:
        #     raise StateNotAttachedError('Cannot call stateful function without attached state')
        try:
            return await self.run(*args)
        except Exception as e:
            logging.error(str(e))
            return e

    async def get(self, key):
        return await self.state.get(key, self.t_id)

    async def put(self, key, value):
        await self.state.put(key, value, self.t_id)

    async def call_remote_function_no_response(self, operator_name, function_name, key, params):
        partition, payload, operator_host, operator_port = await self.prepare_message_transmission(operator_name,
                                                                                                   key,
                                                                                                   function_name,
                                                                                                   params)
        await self.networking.send_message(operator_host,
                                           operator_port,
                                           {"__COM_TYPE__": 'RUN_FUN_REMOTE',
                                            "__MSG__": payload},
                                           Serializer.MSGPACK)

    async def call_remote_function_request_response(self, operator_name, function_name, key, params):
        partition, payload, operator_host, operator_port = await self.prepare_message_transmission(operator_name,
                                                                                                   key,
                                                                                                   function_name,
                                                                                                   params)
        logging.info(f'(SF)  Start {operator_host}:{operator_port} of {operator_name}:{partition}')
        resp = await self.networking.send_message_request_response(operator_host,
                                                                   operator_port,
                                                                   {"__COM_TYPE__": 'RUN_FUN_RQ_RS_REMOTE',
                                                                    "__MSG__": payload},
                                                                   Serializer.MSGPACK)
        return resp

    def attach_state(self, operator_state: State):
        self.state = operator_state

    def attach_networking(self, networking: NetworkingManager):
        self.networking = networking

    def set_timestamp(self, timestamp: int):
        self.timestamp = timestamp

    def set_dns(self, dns: dict[str, dict[str, tuple[str, int]]]):
        self.dns = dns

    def set_t_id(self, t_id: int):
        self.t_id = t_id

    async def prepare_message_transmission(self, operator_name: str, key, function_name: str, params):
        if operator_name not in self.dns:
            logging.error(f"Couldn't find operator: {operator_name} in {self.dns}")

        partition: str = str(int(make_key_hashable(key)) % len(self.dns[operator_name].keys()))

        payload = {'__T_ID__': self.t_id,
                   '__OP_NAME__': operator_name,
                   '__FUN_NAME__': function_name,
                   '__KEY__': key,
                   '__PARTITION__': int(partition),
                   '__TIMESTAMP__': self.timestamp,
                   '__PARAMS__': params}

        operator_host, operator_port = self.dns[operator_name][partition][0], self.dns[operator_name][partition][1]
        return partition, payload, operator_host, operator_port

    @abstractmethod
    async def run(self, *args):
        raise NotImplementedError
