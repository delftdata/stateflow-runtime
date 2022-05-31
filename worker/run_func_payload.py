from dataclasses import dataclass


@dataclass
class RunFuncPayload:
    key: object
    timestamp: int
    operator_name: str
    partition: int
    function_name: str
    params: tuple
    response_socket: object


@dataclass
class SequencedItem:
    t_id: int
    payload: RunFuncPayload

    def __hash__(self):
        return hash(self.t_id)
