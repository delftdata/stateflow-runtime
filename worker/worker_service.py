import asyncio
import os
import socket

import aiojobs
import aiozmq
import uvloop
import zmq

from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.networking import NetworkingManager
from universalis.common.logging import logging
from universalis.common.operator import Operator
from universalis.common.serialization import Serializer

from worker.operator_state.in_memory_state import InMemoryOperatorState
from worker.operator_state.redis_state import RedisOperatorState

SERVER_PORT = 8888
DISCOVERY_HOST = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT = int(os.environ['DISCOVERY_PORT'])
INTERNAL_WATERMARK_SECONDS = 0.005  # 5ms
queue_lock = asyncio.Lock()
network_buffer = []
registered_operators: dict[str, dict[int, Operator]] = {}
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


async def run_function(router, networking_manager, timestamp, operator_name, partition, function_name, params, response_socket):
    logging.info(registered_operators)
    registered_operators[operator_name][partition].set_function_timestamp(function_name, timestamp)
    registered_operators[operator_name][partition].set_function_dns(function_name, dns)
    response = await registered_operators[operator_name][partition].run_function(function_name, *params)
    if response_socket:
        router.write((response_socket, networking_manager.encode_message(response, Serializer.MSGPACK)))


async def get_registered_operators(networking_manager):
    global dns
    dns = await networking_manager.send_message_request_response(DISCOVERY_HOST,
                                                                 DISCOVERY_PORT,
                                                                 "",
                                                                 "",
                                                                 {"__COM_TYPE__": "DISCOVER", "__MSG__": ""},
                                                                 Serializer.MSGPACK)
    logging.info(dns)


async def process_queues(router, scheduler, networking):
    global network_buffer, dns
    while True:
        await asyncio.sleep(INTERNAL_WATERMARK_SECONDS)
        if len(network_buffer) > 0:
            async with queue_lock:
                for queue_entry in network_buffer:
                    timestamp, operator_name, partition, function_name, key, params, response_socket = queue_entry
                    if operator_name not in dns:
                        await scheduler.spawn(get_registered_operators(networking))
                    await scheduler.spawn(run_function(router, networking, timestamp, operator_name,
                                                       partition, function_name, params, response_socket))
                network_buffer = []


async def register_to_coordinator(networking: NetworkingManager):
    await networking.send_message(DISCOVERY_HOST,
                                  DISCOVERY_PORT, "", "",
                                  {"__COM_TYPE__": 'REGISTER_WORKER',
                                   "__MSG__": str(socket.gethostbyname(socket.gethostname()))},
                                  Serializer.MSGPACK)


async def worker_controller(deserialized_data, resp_adr, networking):
    message_type: str = deserialized_data['__COM_TYPE__']
    message = deserialized_data['__MSG__']
    if message_type in ['RUN_FUN', 'RUN_FUN_RQ_RS']:
        # Add message to queue
        async with queue_lock:
            if message_type == 'RUN_FUN':
                network_buffer.append(unpack_run_payload(message))
            else:
                network_buffer.append(unpack_run_payload(message, resp_adr))  # REQUEST RESPONSE
    elif message_type == 'RECEIVE_EXE_PLN':  # RECEIVE EXECUTION PLAN OF A DATAFLOW GRAPH
        operator: Operator
        operator, partition = message
        if operator.name in registered_operators:
            registered_operators[operator.name].update({partition: operator})
            attach_state_to_operator(registered_operators[operator.name][partition], networking)
        else:
            registered_operators[operator.name] = {partition: operator}
            attach_state_to_operator(registered_operators[operator.name][partition], networking)
        logging.info(f'Registered operators: {registered_operators}')
    else:
        logging.error(f"Worker Service: Non supported command message type: {message_type}")


async def main():
    router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{SERVER_PORT}")
    scheduler = await aiojobs.create_scheduler(limit=None)
    networking = NetworkingManager()
    await register_to_coordinator(networking)
    await scheduler.spawn(process_queues(router, scheduler, networking))
    logging.info(f"Worker TCP Server listening at 0.0.0.0:{SERVER_PORT} IP:{socket.gethostbyname(socket.gethostname())}")
    while True:
        resp_adr, data = await router.read()
        deserialized_data: dict = networking.decode_message(data)
        if '__COM_TYPE__' not in deserialized_data:
            logging.error(f"Deserialized data do not contain a message type")
        else:
            await worker_controller(deserialized_data, resp_adr, networking)


def unpack_run_payload(message: dict, response_socket=None):
    return message['__TIMESTAMP__'], message['__OP_NAME__'], message['__PARTITION__'], message['__FUN_NAME__'], \
           message['__KEY__'], message['__PARAMS__'], response_socket


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
