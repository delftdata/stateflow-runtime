import uvloop
import cloudpickle

from asyncio import PriorityQueue, sleep, get_running_loop, run, Future

from websockets import serve

from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.logging import logging
from universalis.common.operator import Operator

from worker.operator_state.in_memory_state import InMemoryOperatorState
from worker.operator_state.redis_state import RedisOperatorState

SERVER_PORT = 8888
INTERNAL_WATERMARK_SECONDS = 0.005  # 5ms


def attach_state_to_operator(operator: Operator):
    if operator.operator_state_backend == LocalStateBackend.DICT:
        state = InMemoryOperatorState()
        operator.attach_state_to_functions(state)
    elif operator.operator_state_backend == LocalStateBackend.REDIS:
        state = RedisOperatorState()
        operator.attach_state_to_functions(state)
    else:
        logging.error(f"Invalid operator state backend type: {operator.operator_state_backend}")


async def receive_data(websocket, _):
    async for msg in websocket:
        deserialized_message: dict = cloudpickle.loads(msg)
        logging.info('RECEIVED MESSAGE')
        if '__COM_TYPE__' not in deserialized_message:
            logging.error(f"Deserialized data do not contain a message type")
        else:
            message_type: str = deserialized_message['__COM_TYPE__']
            message = deserialized_message['__MSG__']
            if message_type == 'RUN_FUN':
                operator_name: str = message['__OP_NAME__']
                partition: int = message['__PARTITION__']
                function_name: str = message['__FUN_NAME__']
                function_params = message['__PARAMS__']
                timestamp: int = message['__TIMESTAMP__']
                logging.debug(f"Running {operator_name}|{partition}:{function_name} "
                              f"with params: {function_params} at time: {timestamp}")
                queue_entry = timestamp, function_name, function_params
                await operator_queues[operator_name][partition].put(queue_entry)
            elif message_type == 'RECEIVE_EXE_PLN':
                # Receive operator from coordinator
                operator: Operator
                operator, partition = message
                if operator.name in registered_operators:
                    registered_operators[operator.name].update({partition: operator})
                    attach_state_to_operator(registered_operators[operator.name][partition])
                    operator_queues[operator.name].update({partition: PriorityQueue()})
                else:
                    registered_operators[operator.name] = {partition: operator}
                    attach_state_to_operator(registered_operators[operator.name][partition])
                    operator_queues[operator.name] = {partition: PriorityQueue()}
                logging.info(f'Registered operators: {registered_operators}')
            else:
                logging.error(f"TCP SERVER: Non supported message type: {message_type}")


async def process_queue():
    while True:
        for operator_name, partitioned_queues in operator_queues.items():
            for partition, q in partitioned_queues.items():
                while not q.empty():
                    queue_value = await q.get()
                    timestamp, function_name, params = queue_value
                    logging.debug(f'Running function {function_name} with params {params} at {timestamp}')
                    registered_operators[operator_name][partition].set_function_timestamp(function_name, timestamp)
                    await registered_operators[operator_name][partition].run_function(function_name, *params)
        await sleep(INTERNAL_WATERMARK_SECONDS)


async def main():
    async with serve(receive_data, "0.0.0.0", SERVER_PORT):
        logging.info(f"Worker Service listening at 0.0.0.0:{SERVER_PORT}")
        loop = get_running_loop()
        loop.create_task(process_queue())
        logging.info('Queue ingestion timer registered')
        await Future()  # run forever

if __name__ == "__main__":
    registered_operators: dict[str, dict[int, Operator]] = {}
    operator_queues: dict[str, dict[int, PriorityQueue]] = {}
    uvloop.install()
    run(main())
