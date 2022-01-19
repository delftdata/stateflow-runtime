import asyncio
import os

import aiojobs
import aiozmq
import zmq
import uvloop

from universalis.common.logging import logging
from universalis.common.networking import NetworkingManager
from universalis.common.operator import Operator
from universalis.common.serialization import Serializer
from universalis.common.local_state_backends import LocalStateBackend

from worker.operator_state.in_memory_state import InMemoryOperatorState
from worker.operator_state.redis_state import RedisOperatorState

registered_operators: dict[str, dict[int, Operator]] = {}
DISCOVERY_HOST = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT = int(os.environ['DISCOVERY_PORT'])
dns = {}


def attach_state_to_operator(operator: Operator, networking):
    if operator.operator_state_backend == LocalStateBackend.DICT:
        state = InMemoryOperatorState()
        operator.attach_state_to_functions(state, networking)
    elif operator.operator_state_backend == LocalStateBackend.REDIS:
        state = RedisOperatorState()
        operator.attach_state_to_functions(state, networking)
    else:
        logging.error(f"Invalid operator state backend type: {operator.operator_state_backend}")


async def get_registered_operators(networking_manager):
    global dns
    dns = await networking_manager.send_message_request_response(DISCOVERY_HOST,
                                                                 DISCOVERY_PORT,
                                                                 "",
                                                                 "",
                                                                 {"__COM_TYPE__": "DISCOVER", "__MSG__": ""},
                                                                 Serializer.MSGPACK)
    logging.info(dns)


async def run_function(timestamp, operator_name, partition, function_name, params):
    logging.info(registered_operators)
    registered_operators[operator_name][partition].set_function_timestamp(function_name, timestamp)
    registered_operators[operator_name][partition].set_function_dns(function_name, dns)
    await registered_operators[operator_name][partition].run_function(function_name, *params)


async def start_executor():
    global dns
    networking_manager: NetworkingManager = NetworkingManager()
    router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind="ipc://executor")
    scheduler = await aiojobs.create_scheduler(limit=None)
    logging.info(f"Worker Execution Server started")
    while True:
        resp_adr, data = await router.read()
        deserialized_data: dict = networking_manager.decode_message(data)
        if '__COM_TYPE__' not in deserialized_data:
            logging.error(f"Deserialized data do not contain a message type")
        else:
            message_type: str = deserialized_data['__COM_TYPE__']
            message = deserialized_data['__MSG__']
            if message_type == 'BATCH':
                for queue_entry in message:
                    timestamp, operator_name, partition, function_name, key, params = queue_entry
                    if operator_name not in dns:
                        await scheduler.spawn(get_registered_operators(networking_manager))
                        await asyncio.sleep(1)
                    await scheduler.spawn(run_function(timestamp, operator_name, partition, function_name, params))
            elif message_type == 'RQ_RS':
                timestamp, operator_name, partition, function_name, key, params = message
                if operator_name not in dns:
                    await scheduler.spawn(get_registered_operators(networking_manager))
                    await asyncio.sleep(1)
                registered_operators[operator_name][partition].set_function_timestamp(function_name, timestamp)
                registered_operators[operator_name][partition].set_function_dns(function_name, dns)
                res = await registered_operators[operator_name][partition].run_function(function_name, *params)
                router.write((resp_adr, networking_manager.encode_message(res, Serializer.MSGPACK)))
            elif message_type == 'RECEIVE_EXE_PLN':  # RECEIVE EXECUTION PLAN OF A DATAFLOW GRAPH
                # Receive operator from coordinator
                operator: Operator
                operator, partition = message
                if operator.name in registered_operators:
                    registered_operators[operator.name].update({partition: operator})
                    attach_state_to_operator(registered_operators[operator.name][partition], networking_manager)
                else:
                    registered_operators[operator.name] = {partition: operator}
                    attach_state_to_operator(registered_operators[operator.name][partition], networking_manager)
                logging.info(f'Registered operators: {registered_operators}')
            else:
                logging.error(f"Worker Service: Non supported command message type: {message_type}")


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(start_executor())
