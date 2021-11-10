import os
import uvloop
import asyncio

from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.logging import logging
from universalis.common.networking import decode_messages
from universalis.common.operator import Operator

from worker.operator_state.in_memory_state import InMemoryOperatorState
from worker.operator_state.redis_state import RedisOperatorState

SERVER_PORT = 8888
DISCOVERY_HOST = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT = int(os.environ['DISCOVERY_PORT'])
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


class WorkerServerProtocol(asyncio.Protocol):

    def __init__(self):
        self.transport: asyncio.Transport = asyncio.Transport()

    def connection_made(self, transport: asyncio.Transport):
        peername = transport.get_extra_info('peername')
        logging.info(f"Connection from {peername}")
        self.transport = transport

    def data_received(self, data):
        global registered_operator_connections
        deserialized_data: dict
        for deserialized_data in decode_messages(data):
            logging.info('RECEIVED MESSAGE')
            if '__COM_TYPE__' not in deserialized_data:
                logging.error(f"Deserialized data do not contain a message type")
            else:
                message_type: str = deserialized_data['__COM_TYPE__']
                message = deserialized_data['__MSG__']
                if message_type == 'RUN_FUN':
                    operator_name: str = message['__OP_NAME__']
                    partition: int = message['__PARTITION__']
                    function_name: str = message['__FUN_NAME__']
                    function_params = message['__PARAMS__']
                    timestamp: int = message['__TIMESTAMP__']
                    logging.debug(f"Running {operator_name}|{partition}:{function_name} "
                                  f"with params: {function_params} at time: {timestamp}")
                    queue_entry = timestamp, function_name, function_params
                    operator_queues[operator_name][partition].put_nowait(queue_entry)
                elif message_type == 'RECEIVE_EXE_PLN':
                    # Receive operator from coordinator
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
                else:
                    logging.error(f"TCP SERVER: Non supported message type: {message_type}")


async def process_queue():
    while True:
        for operator_name, partitioned_queues in operator_queues.items():
            for partition, q in partitioned_queues.items():
                while not q.empty():
                    queue_value = await q.get()
                    timestamp, function_name, params = queue_value
                    logging.info(f'Running function {function_name} with params {params} at {timestamp}')
                    registered_operators[operator_name][partition].set_function_timestamp(function_name, timestamp)
                    await registered_operators[operator_name][partition].run_function(function_name, *params)
        await asyncio.sleep(INTERNAL_WATERMARK_SECONDS)


async def main():
    loop = asyncio.get_running_loop()
    server = await loop.create_server(lambda: WorkerServerProtocol(), '0.0.0.0', SERVER_PORT)
    logging.info(f"Worker Service listening at 0.0.0.0:{SERVER_PORT}")

    loop = asyncio.get_running_loop()
    loop.create_task(process_queue())
    logging.info('Queue ingestion timer registered')

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    registered_operators: dict[str, dict[int, Operator]] = {}
    operator_queues: dict[str, dict[int, asyncio.PriorityQueue]] = {}
    registered_operator_connections: dict[dict[str, tuple[str, int]]] = {}
    uvloop.install()
    asyncio.run(main())
