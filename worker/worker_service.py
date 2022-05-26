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
from worker.run_func_payload import RunFuncPayload
from worker.sequencer.sequencer import Sequencer

SERVER_PORT = 8888
DISCOVERY_HOST = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT = int(os.environ['DISCOVERY_PORT'])
KAFKA_URL: str = os.getenv('KAFKA_URL', None)
INGRESS_TYPE = os.getenv('INGRESS_TYPE', None)
EGRESS_TOPIC_NAME: str = 'universalis-egress'
EPOCH_INTERVAL = 0.01  # 10ms


class Worker:

    def __init__(self):
        self.id: int = -1
        self.networking = NetworkingManager()
        self.sequencer = Sequencer()
        self.scheduler = None
        self.router = None
        self.kafka_egress_producer = None
        self.registered_operators: dict[str, dict[int, Operator]] = {}
        self.dns: dict[str, dict[str, tuple[str, int]]] = {}
        self.topic_partitions: list[TopicPartition] = []
        self.peers: dict[int, tuple[str, int]] = {}  # worker_id: (host, port)
        # ready_to_commit_events -> worker_id: Event that appears if the peer is ready to commit
        self.ready_to_commit_events: dict[int, asyncio.Event] = {}
        self.sequences: dict[int, list] = {}
        self.aborted_transactions = []
        self.local_state = None

    def attach_state_to_operator(self, operator: Operator):
        # TODO The entire dataflow graph should have the same state backend (must change the client api)
        if operator.operator_state_backend == LocalStateBackend.DICT:
            if self.local_state is None:
                self.local_state = InMemoryOperatorState()
            operator.attach_state_to_functions(self.local_state, self.networking)
        elif operator.operator_state_backend == LocalStateBackend.REDIS:
            if self.local_state is None:
                self.local_state = RedisOperatorState()
            operator.attach_state_to_functions(self.local_state, self.networking)
        else:
            logging.error(f"Invalid operator state backend type: {operator.operator_state_backend}")

    async def run_function(self, timestamp, operator_name,
                           partition, function_name, params, response_socket):
        logging.info(self.registered_operators)
        self.registered_operators[operator_name][partition].set_function_timestamp(function_name, timestamp)
        self.registered_operators[operator_name][partition].set_function_dns(function_name, self.dns)
        response = await self.registered_operators[operator_name][partition].run_function(function_name, *params)
        if response_socket:
            self.router.write((response_socket, self.networking.encode_message(response,
                                                                               Serializer.MSGPACK)))
        elif not response_socket and response:
            await self.kafka_egress_producer.send_and_wait(EGRESS_TOPIC_NAME,
                                                           value=self.networking.encode_message(response,
                                                                                                Serializer.MSGPACK))

    async def send_commit_to_peers(self, epoch_sequence):
        # TODO WAS SEND SEQ ADAPT FOR COMMIT
        for worker_id, url in self.peers.items():
            await self.scheduler.spawn(self.networking.send_message_request_response(
                url[0], url[0], "", "",
                {"__COM_TYPE__": 'SEQ_PART',
                 "__MSG__": (self.id, epoch_sequence)},
                Serializer.MSGPACK))

    def merge_sub_sequences(self) -> list:
        return sorted(self.sequences.values())

    async def function_scheduler(self):  # TODO Scheduler logic here
        while True:
            await asyncio.sleep(EPOCH_INTERVAL)  # EPOCH TIMER
            self.sequences[self.id] = await self.sequencer.get_epoch()  # GET SEQUENCE
            # await self.send_epoch_to_peers(self.sequences[self.id])  # SEND SEQUENCE TO PEERS
            # wait_remote_sequences_task = [asyncio.ensure_future(event.wait())  # WAIT FOR THE EVENTS
            #                               for event in self.rcv_sequences_events.values()]
            # await asyncio.gather(*wait_remote_sequences_task)
            # MERGE SEQUENCES
            # global_sequence = self.merge_sub_sequences()
            # if len(self.sequences[self.id]) > 0:
            #     for queue_entry in self.sequences[self.id]:
            #         queue_entry: RunFuncPayload
            #         if queue_entry.operator_name not in self.dns:
            #             # TODO MUST REMOVE THIS, MAKE THE JOB SUBMISSION SMOOTH
            #             await self.scheduler.spawn(self.get_registered_operators(self.networking))
            #         await self.scheduler.spawn(self.run_function(queue_entry))
            # for event in self.rcv_sequences_events.values():
            #     event.clear()

    async def start_kafka_egress_producer(self):
        self.kafka_egress_producer = AIOKafkaProducer(bootstrap_servers=[KAFKA_URL])
        while True:
            try:
                await self.kafka_egress_producer.start()
            except KafkaConnectionError:
                time.sleep(1)
                logging.info("Waiting for Kafka")
                continue
            break

    async def start_kafka_consumer(self, topic_partitions: list[TopicPartition]):
        logging.info(f'Creating Kafka consumer for topic partitions: {topic_partitions}')
        consumer = AIOKafkaConsumer(bootstrap_servers=[KAFKA_URL])
        consumer.assign(topic_partitions)
        while True:
            # start the kafka consumer
            try:
                await consumer.start()
            except (UnknownTopicOrPartitionError, KafkaConnectionError):
                time.sleep(1)
                logging.warning(f'Kafka at {KAFKA_URL} not ready yet, sleeping for 1 second')
                continue
            break
        try:
            # Consume messages
            async for msg in consumer:
                logging.info(f"Consumed: {msg.topic} {msg.partition} {msg.offset} "
                             f"{msg.key} {msg.value} {msg.timestamp}")
                deserialized_data: dict = self.networking.decode_message(msg.value)
                message_type: str = deserialized_data['__COM_TYPE__']
                message = deserialized_data['__MSG__']
                if message_type == 'RUN_FUN':
                    run_func_payload: RunFuncPayload = self.unpack_run_payload(message)
                    await self.sequencer.sequence(run_func_payload)
                else:
                    logging.error(f"Invalid message type: {message_type} passed to KAFKA")
        finally:
            await consumer.stop()

    async def worker_controller(self, deserialized_data, resp_adr):
        message_type: str = deserialized_data['__COM_TYPE__']
        message = deserialized_data['__MSG__']
        match message_type:
            case 'RUN_FUN' | 'RUN_FUN_RQ_RS':  # TODO RQ_RS for remote reads RUN_FUN for imediate execution not sequence
                if message_type == 'RUN_FUN':
                    await self.sequencer.sequence(self.unpack_run_payload(message))
                else:
                    await self.sequencer.sequence(self.unpack_run_payload(message, resp_adr))  # REQUEST RESPONSE
            case 'RECEIVE_EXE_PLN':  # RECEIVE EXECUTION PLAN OF A DATAFLOW GRAPH
                # This contains all the operators of a job assigned to this worker
                await self.handle_execution_plan(message)
            case 'SEQ_PART':
                remote_worker_id, remote_sequence = message
                self.rcv_sequences_events[remote_worker_id].set()
                self.sequences[remote_worker_id] = remote_sequence
            case _:
                logging.error(f"Worker Service: Non supported command message type: {message_type}")

    async def handle_execution_plan(self, message):
        worker_operators, self.dns, self.peers = message
        self.sequencer.set_n_workers(len(self.peers))
        del self.peers[self.id]
        operator: Operator
        for tup in worker_operators:
            operator, partition = tup
            if operator.name in self.registered_operators:
                self.registered_operators[operator.name].update({partition: operator})
                self.attach_state_to_operator(self.registered_operators[operator.name][partition])
            else:
                self.registered_operators[operator.name] = {partition: operator}
                self.attach_state_to_operator(self.registered_operators[operator.name][partition])
            if INGRESS_TYPE == 'KAFKA':
                self.topic_partitions.append(TopicPartition(operator.name, partition))
        await self.scheduler.spawn(self.start_kafka_consumer(self.topic_partitions))
        logging.info(f'Registered operators: {self.registered_operators} \n'
                     f'Peers: {self.peers} \n'
                     f'Operator locations: {self.dns}')

    async def start_tcp_service(self):
        self.router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{SERVER_PORT}")
        await self.start_kafka_egress_producer()
        await self.scheduler.spawn(self.function_scheduler())
        logging.info(f"Worker TCP Server listening at 0.0.0.0:{SERVER_PORT} "
                     f"IP:{socket.gethostbyname(socket.gethostname())}")
        while True:
            resp_adr, data = await self.router.read()
            deserialized_data: dict = self.networking.decode_message(data)
            if '__COM_TYPE__' not in deserialized_data:
                logging.error(f"Deserialized data do not contain a message type")
            else:
                await self.worker_controller(deserialized_data, resp_adr)

    @staticmethod
    def unpack_run_payload(message: dict, response_socket=None) -> RunFuncPayload:
        return RunFuncPayload(message['__KEY__'], message['__TIMESTAMP__'], message['__OP_NAME__'],
                              message['__PARTITION__'], message['__FUN_NAME__'],
                              message['__PARAMS__'], response_socket)

    async def register_to_coordinator(self):
        self.id = await self.networking.send_message_request_response(
            DISCOVERY_HOST, DISCOVERY_PORT, "", "",
            {"__COM_TYPE__": 'REGISTER_WORKER',
             "__MSG__": str(socket.gethostbyname(socket.gethostname()))},
            Serializer.MSGPACK)
        self.sequencer.set_worker_id(self.id)
        logging.info(f"Worker id: {self.id}")

    async def main(self):
        self.scheduler = await aiojobs.create_scheduler(limit=None)
        await self.register_to_coordinator()
        await self.start_tcp_service()


if __name__ == "__main__":
    uvloop.install()
    worker = Worker()
    asyncio.run(Worker().main())
