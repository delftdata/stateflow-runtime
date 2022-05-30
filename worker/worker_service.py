import asyncio
import itertools
import os
import socket
import time
from typing import Union

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
from worker.run_func_payload import RunFuncPayload, RunRemoteFuncPayload
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
        self.aborted_from_remote: dict[int, list] = {}
        self.logic_aborts = []
        self.logic_aborts_from_remote: dict[int, list] = {}
        self.local_state: Union[InMemoryOperatorState, RedisOperatorState] = None

    def attach_state_to_operator(self, operator: Operator):
        # TODO The entire dataflow graph should have the same state backend for aria to work
        #  (must change the client api)
        if operator.operator_state_backend == LocalStateBackend.DICT:
            if self.local_state is None:
                self.local_state = InMemoryOperatorState()
            operator.attach_state_networking(self.local_state, self.networking, self.dns)
        elif operator.operator_state_backend == LocalStateBackend.REDIS:
            if self.local_state is None:
                self.local_state = RedisOperatorState()
            operator.attach_state_networking(self.local_state, self.networking, self.dns)
        else:
            logging.error(f"Invalid operator state backend type: {operator.operator_state_backend}")

    async def run_function(self, t_id: int, payload: RunFuncPayload):
        # TODO This should be re-evaluated
        operator_partition = self.registered_operators[payload.operator_name][payload.partition]
        response = await operator_partition.run_function(t_id,
                                                         payload.timestamp,
                                                         payload.function_name,
                                                         *payload.params)
        if isinstance(response, Exception):
            self.logic_aborts.append(t_id)

        if payload.response_socket:
            self.router.write((payload.response_socket, self.networking.encode_message(response,
                                                                                       Serializer.MSGPACK)))
        elif not payload.response_socket and response:
            await self.kafka_egress_producer.send_and_wait(EGRESS_TOPIC_NAME,
                                                           value=self.networking.encode_message(response,
                                                                                                Serializer.MSGPACK))

    async def send_commit_to_peers(self, aborted):
        for worker_id, url in self.peers.items():
            await self.scheduler.spawn(self.networking.send_message(url[0], url[1],
                                                                    {"__COM_TYPE__": 'CMT',
                                                                     "__MSG__": (self.id, aborted, self.logic_aborts)},
                                                                    Serializer.MSGPACK))

    async def function_scheduler(self):
        while True:
            await asyncio.sleep(EPOCH_INTERVAL)  # EPOCH TIMER
            sequence = await self.sequencer.get_epoch()  # GET SEQUENCE
            if sequence is not None:
                run_function_tasks = []
                for t_id, payload in sequence:
                    run_function_tasks.append(asyncio.ensure_future(self.run_function(t_id, payload)))
            # TODO wait for commit messages with all the conflicts
            # TODO resolve local concurrency conflicts that came as a result of remote and add them to the next sequence
            # TODO error conflicts should go to the egress as 400
            # TODO batch producer in the universalis client
                await asyncio.gather(*run_function_tasks)
                aborted = self.local_state.check_conflicts()
                logging.warning(f'Aborted: {aborted}')
                await self.send_commit_to_peers(aborted)
                logging.warning(self.ready_to_commit_events)
                wait_remote_commits_task = [asyncio.ensure_future(event.wait())
                                            for event in self.ready_to_commit_events.values()]
                logging.warning(f'Waiting on remote commits...')
                await asyncio.gather(*wait_remote_commits_task)
                # TODO ADD THE CONCURRENCY ABORTS BACK TO THE SEQUENCER
                remote_aborted = list(itertools.chain.from_iterable(self.aborted_from_remote.values()))
                logic_aborts_everywhere = list(itertools.chain.from_iterable(self.logic_aborts_from_remote.values()))
                logic_aborts_everywhere.extend(self.logic_aborts)
                aborts = remote_aborted + logic_aborts_everywhere
                logging.warning(f'Commits received!')
                await self.local_state.commit(aborts)
                # TODO make sure of the cleanup
                for event in self.ready_to_commit_events.values():
                    event.clear()
                self.logic_aborts = []

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
            wait_time_ms = int(EPOCH_INTERVAL*1000)
            while True:
                result = await consumer.getmany(timeout_ms=wait_time_ms)
                for _, messages in result.items():
                    if messages:
                        for msg in messages:
                            logging.info(f"Consumed: {msg.topic} {msg.partition} {msg.offset} "
                                         f"{msg.key} {msg.value} {msg.timestamp}")
                            deserialized_data: dict = self.networking.decode_message(msg.value)
                            message_type: str = deserialized_data['__COM_TYPE__']
                            message = deserialized_data['__MSG__']
                            if message_type == 'RUN_FUN':
                                run_func_payload: RunFuncPayload = self.unpack_run_payload(message)
                                logging.warning(f'SEQ FROM KAFKA: {run_func_payload.function_name}')
                                await self.sequencer.sequence(run_func_payload)
                            else:
                                logging.error(f"Invalid message type: {message_type} passed to KAFKA")
        finally:
            await consumer.stop()

    async def worker_controller(self, deserialized_data, resp_adr):
        message_type: str = deserialized_data['__COM_TYPE__']
        message = deserialized_data['__MSG__']
        match message_type:
            case 'RUN_FUN_REMOTE' | 'RUN_FUN_RQ_RS_REMOTE':  # TODO RQ_RS for remote reads RUN_FUN for imediate execution not sequence
                if message_type == 'RUN_FUN_REMOTE':
                    logging.warning('CALLED RUN FUN FROM PEER')
                    await self.sequencer.sequence(self.unpack_run_remote_payload(message))
                else:
                    logging.warning('CALLED RUN FUN RQ RS FROM PEER')
                    await self.sequencer.sequence(self.unpack_run_remote_payload(message, resp_adr))  # REQUEST RESPONSE
            case 'RECEIVE_EXE_PLN':  # RECEIVE EXECUTION PLAN OF A DATAFLOW GRAPH
                # This contains all the operators of a job assigned to this worker
                await self.handle_execution_plan(message)
            case 'CMT':
                remote_worker_id, aborted, logic_aborts = message
                self.ready_to_commit_events[remote_worker_id].set()
                self.aborted_from_remote[remote_worker_id] = aborted
                self.logic_aborts_from_remote[remote_worker_id] = logic_aborts
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
        self.ready_to_commit_events = {peer_id: asyncio.Event() for peer_id in self.peers.keys()}
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

    @staticmethod
    def unpack_run_remote_payload(message: dict, response_socket=None) -> RunRemoteFuncPayload:
        return RunRemoteFuncPayload(message['__KEY__'], message['__TIMESTAMP__'],
                                    message['__T_ID__'], message['__OP_NAME__'],
                                    message['__PARTITION__'], message['__FUN_NAME__'],
                                    message['__PARAMS__'], response_socket)

    async def register_to_coordinator(self):
        self.id = await self.networking.send_message_request_response(
            DISCOVERY_HOST, DISCOVERY_PORT,
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
