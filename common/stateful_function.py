from abc import abstractmethod

from common.function import Function
from common.state import OperatorState


class StateNotAttachedError(Exception):
    pass


class StatefulFunction(Function):

    state: OperatorState

    def __init__(self):
        super().__init__()

    def __call__(self, *args, **kwargs):
        if self.state is None:
            raise StateNotAttachedError('Cannot call stateful function without attached state')
        return self.run(*args)

    def attach_state(self, operator_state: OperatorState):
        self.state = operator_state

    @abstractmethod
    def run(self, *args):
        raise NotImplementedError
