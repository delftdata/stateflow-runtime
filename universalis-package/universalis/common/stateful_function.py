import os
import uuid
from abc import abstractmethod


from .networking import async_transmit_tcp_no_response, async_transmit_tcp_request_response
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
        self.timestamp = None

    async def __call__(self, *args, **kwargs):
        if self.state is None:
            raise StateNotAttachedError('Cannot call stateful function without attached state')
        try:
            return await self.run(*args)
        except TypeError:
            pass

    async def call_remote_function_no_response(self, operator_name, function_name, key, params):

        if operator_name not in self.dns:
            self.dns = await async_transmit_tcp_request_response(os.environ['DISCOVERY_HOST'],
                                                                 int(os.environ['DISCOVERY_PORT']),
                                                                 "",
                                                                 com_type="DISCOVER")

        partition: str = str(int(make_key_hashable(key)) % len(self.dns[operator_name].keys()))

        payload = {'__OP_NAME__': operator_name,
                   '__FUN_NAME__': function_name,
                   '__PARTITION__': int(partition),
                   '__TIMESTAMP__': self.timestamp,
                   '__PARAMS__': params}

        await async_transmit_tcp_no_response(self.dns[operator_name][partition][0],
                                             self.dns[operator_name][partition][1],
                                             payload,
                                             com_type='RUN_FUN')

    def attach_state(self, operator_state: State):
        self.state = operator_state

    def set_timestamp(self, timestamp: int):
        self.timestamp = timestamp

    @abstractmethod
    async def run(self, *args):
        raise NotImplementedError
