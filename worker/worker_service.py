import asyncio
import os
import socket
import time

import aiojobs
import aiozmq
import uvloop
import zmq
from aiokafka import AIOKafkaConsumer, TopicPartition, AIOKafkaProducer
from aiokafka.errors import UnknownTopicOrPartitionError, KafkaConnectionError

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
dns: dict[str, dict[str, tuple[str, int]]] = {}
kafka_url: str = os.getenv('KAFKA_URL', None)


def attach_state_to_operator(operator: Operator, networking):
    if operator.operator_state_backend == LocalStateBackend.DICT:
        state = InMemoryOperatorState()
        operator.attach_state_to_functions(state, networking)
    elif operator.operator_state_backend == LocalStateBackend.REDIS:
        state = RedisOperatorState()
        operator.attach_state_to_functions(state, networking)
    else:
        logging.error(f"Invalid operator state backend type: {operator.operator_state_backend}")


async def run_function(router, networking_manager, timestamp, operator_name,
                       partition, function_name, params, response_socket, kafka_egress_producer):
    logging.info(registered_operators)
    registered_operators[operator_name][partition].set_function_timestamp(function_name, timestamp)
    registered_operators[operator_name][partition].set_function_dns(function_name, dns)
    response = await registered_operators[operator_name][partition].run_function(function_name, *params)
    if response_socket:
        router.write((response_socket, networking_manager.encode_message(response, Serializer.MSGPACK)))
    elif not response_socket and response:
        await kafka_egress_producer.send_and_wait('universalis-egress',
                                                  value=networking_manager.encode_message(response, Serializer.MSGPACK))


async def get_registered_operators(networking_manager):
    global dns
    dns = await networking_manager.send_message_request_response(DISCOVERY_HOST,
                                                                 DISCOVERY_PORT,
                                                                 "",
                                                                 "",
                                                                 {"__COM_TYPE__": "DISCOVER", "__MSG__": ""},
                                                                 Serializer.MSGPACK)
    logging.info(dns)


async def process_queues(router, scheduler, networking, kafka_egress_producer):
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
                                                       partition, function_name, params,
                                                       response_socket, kafka_egress_producer))
                network_buffer = []


async def register_to_coordinator(networking: NetworkingManager):
    await networking.send_message(DISCOVERY_HOST,
                                  DISCOVERY_PORT, "", "",
                                  {"__COM_TYPE__": 'REGISTER_WORKER',
                                   "__MSG__": str(socket.gethostbyname(socket.gethostname()))},
                                  Serializer.MSGPACK)


async def start_kafka_egress_producer():
    kafka_egress_producer = AIOKafkaProducer(bootstrap_servers=[kafka_url])
    while True:
        try:
            await kafka_egress_producer.start()
        except KafkaConnectionError:
            time.sleep(1)
            logging.info("Waiting for Kafka")
            continue
        break
    return kafka_egress_producer


async def start_kafka_consumer(topic_partitions: list[TopicPartition], networking):
    logging.info(f'Creating Kafka consumer for topic partitions: {topic_partitions}')
    consumer = AIOKafkaConsumer(bootstrap_servers=[kafka_url])
    consumer.assign(topic_partitions)
    while True:
        try:
            await consumer.start()
        except (UnknownTopicOrPartitionError, KafkaConnectionError):
            time.sleep(1)
            logging.warning(f'Kafka at {kafka_url} not ready yet, sleeping for 1 second')
            continue
        break
    try:
        # Consume messages
        async for msg in consumer:
            logging.info(f"Consumed: {msg.topic} {msg.partition} {msg.offset} {msg.key} {msg.value} {msg.timestamp}")
            deserialized_data: dict = networking.decode_message(msg.value)
            message_type: str = deserialized_data['__COM_TYPE__']
            message = deserialized_data['__MSG__']
            if message_type == 'RUN_FUN':
                async with queue_lock:
                    network_buffer.append(unpack_run_payload(message))
            else:
                logging.error(f"Invalid message type: {message_type} passed to KAFKA")
    finally:
        await consumer.stop()


async def worker_controller(deserialized_data, resp_adr, networking, scheduler):
    ingress_type = os.getenv('INGRESS_TYPE', None)
    topic_partitions: list[TopicPartition] = []
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
        # This contains all the operators of a job assigned to this worker
        operator: Operator
        for tup in message:
            operator, partition = tup
            if operator.name in registered_operators:
                registered_operators[operator.name].update({partition: operator})
                attach_state_to_operator(registered_operators[operator.name][partition], networking)
            else:
                registered_operators[operator.name] = {partition: operator}
                attach_state_to_operator(registered_operators[operator.name][partition], networking)
            if ingress_type == 'KAFKA':
                topic_partitions.append(TopicPartition(operator.name, partition))
        await scheduler.spawn(start_kafka_consumer(topic_partitions, networking))
        logging.info(f'Registered operators: {registered_operators}')
    else:
        logging.error(f"Worker Service: Non supported command message type: {message_type}")


async def start_tcp_service(scheduler, networking):
    router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{SERVER_PORT}")
    kafka_egress_producer = await start_kafka_egress_producer()
    await scheduler.spawn(process_queues(router, scheduler, networking, kafka_egress_producer))
    logging.info(f"Worker TCP Server listening at 0.0.0.0:{SERVER_PORT} "
                 f"IP:{socket.gethostbyname(socket.gethostname())}")
    while True:
        resp_adr, data = await router.read()
        deserialized_data: dict = networking.decode_message(data)
        if '__COM_TYPE__' not in deserialized_data:
            logging.error(f"Deserialized data do not contain a message type")
        else:
            await worker_controller(deserialized_data, resp_adr, networking, scheduler)


async def main():
    scheduler = await aiojobs.create_scheduler(limit=None)
    networking = NetworkingManager()
    await register_to_coordinator(networking)
    await start_tcp_service(scheduler, networking)


def unpack_run_payload(message: dict, response_socket=None):
    return message['__TIMESTAMP__'], message['__OP_NAME__'], message['__PARTITION__'], message['__FUN_NAME__'], \
           message['__KEY__'], message['__PARAMS__'], response_socket


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
