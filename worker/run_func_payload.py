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
