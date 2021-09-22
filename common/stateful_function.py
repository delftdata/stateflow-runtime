import asyncio
from abc import abstractmethod

import cloudpickle

from common.function import Function
from common.state import OperatorState


class StateNotAttachedError(Exception):
    pass


class StatefulFunction(Function):

    state: OperatorState

    def __init__(self):
        super().__init__()
        self.dns = {}

    async def __call__(self, *args, **kwargs):
        if self.state is None:
            raise StateNotAttachedError('Cannot call stateful function without attached state')
        try:
            return await self.run(*args)
        except TypeError:
            pass

    async def call_remote_function_no_response(self, operator_name, function_name, params):
        _, writer = await asyncio.open_connection(self.dns[operator_name][0], self.dns[operator_name][1])
        payload = {'__OP_NAME__': operator_name, '__FUN_NAME__': function_name, '__PARAMS__': params}
        writer.write(cloudpickle.dumps({"__COM_TYPE__": "REMOTE_RUN_FUN", "__MSG__": payload}))
        await writer.drain()
        writer.close()

    def attach_state(self, operator_state: OperatorState):
        self.state = operator_state

    def set_dns(self, dns):
        self.dns = dns

    @abstractmethod
    async def run(self, *args):
        raise NotImplementedError
