import asyncio

from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.logging import logging
from universalis.common.operator import Operator


from worker.operator_state.in_memory_state import InMemoryOperatorState
from worker.operator_state.redis_state import RedisOperatorState


def attach_state_to_operator(operator: Operator):
    request_response_db: RedisOperatorState = RedisOperatorState(db=1)
    if operator.operator_state_backend == LocalStateBackend.DICT:
        state = InMemoryOperatorState()
        operator.attach_state_to_functions(state, request_response_db)
    elif operator.operator_state_backend == LocalStateBackend.REDIS:
        state = RedisOperatorState()
        operator.attach_state_to_functions(state, request_response_db)
    else:
        logging.error(f"Invalid operator state backend type: {operator.operator_state_backend}")


def run_fun(message, operator_queues, response_host_name=None):
    operator_name: str = message['__OP_NAME__']
    partition: int = message['__PARTITION__']
    function_name: str = message['__FUN_NAME__']
    function_params = message['__PARAMS__']
    timestamp: int = message['__TIMESTAMP__']
    key = message['__KEY__']
    logging.debug(f"Running {operator_name}|{partition}:{function_name} "
                  f"with params: {function_params} at time: {timestamp}")
    queue_entry = timestamp, function_name, key, function_params, response_host_name
    # logging.warning(f'Adding -> {queue_entry}  to the queue')
    operator_queues[operator_name][partition].put_nowait(queue_entry)


def receive_exe_plan(message, registered_operators, operator_queues):
    operator: Operator
    operator, partition = message
    if operator.name in registered_operators:
        registered_operators[operator.name].update({partition: operator})
        attach_state_to_operator(registered_operators[operator.name][partition])
        operator_queues[operator.name].update({partition: asyncio.PriorityQueue()})
    else:
        registered_operators[operator.name] = {partition: operator}
        attach_state_to_operator(registered_operators[operator.name][partition])
        operator_queues[operator.name] = {partition: asyncio.PriorityQueue()}
    logging.info(f'Registered operators: {registered_operators}')
