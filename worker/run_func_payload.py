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
class RunRemoteFuncPayload:
    key: object
    timestamp: int
    t_id: int
    operator_name: str
    partition: int
    function_name: str
    params: tuple
    response_socket: object
