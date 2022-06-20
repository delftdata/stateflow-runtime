import asyncio
import itertools
import os
import time
from timeit import default_timer as timer

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
from universalis.common.serialization import Serializer, msgpack_serialization

from worker.operator_state.in_memory_state import InMemoryOperatorState
from worker.operator_state.redis_state import RedisOperatorState
from worker.operator_state.stateless import Stateless
from worker.run_func_payload import RunFuncPayload, SequencedItem
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
        self.operator_state_backend = None
        self.registered_operators: dict[tuple[str, int], Operator] = {}
        self.dns: dict[str, dict[str, tuple[str, int]]] = {}
        self.topic_partitions: list[TopicPartition] = []
        self.peers: dict[int, tuple[str, int]] = {}  # worker_id: (host, port)
        # ready_to_commit_events -> worker_id: Event that appears if the peer is ready to commit
        self.ready_to_commit_events: dict[int, asyncio.Event] = {}
        self.aborted_from_remote: dict[int, set[int]] = {}
        self.logic_aborts: set[int] = set()
        self.logic_aborts_from_remote: dict[int, set[int]] = {}
        self.local_state: InMemoryOperatorState | RedisOperatorState | Stateless = Stateless()
        self.t_counters: dict[int, int] = {}
        self.response_buffer: dict[int, tuple] = {}  # t_id: (request_id, response)
        self.operator_functions: dict[str, str] = {}  # function_name: operator_name

    async def run_function(self, t_id: int, payload: RunFuncPayload, ack_payload=None):
        operator_partition = self.registered_operators[(payload.operator_name, payload.partition)]
        response = await operator_partition.run_function(t_id,
                                                         payload.request_id,
                                                         payload.timestamp,
                                                         payload.function_name,
                                                         ack_payload,
                                                         *payload.params)

        if payload.response_socket:
            self.router.write((payload.response_socket, self.networking.encode_message(response,
                                                                                       Serializer.MSGPACK)))
        elif not payload.response_socket and response:
            if isinstance(response, Exception):
                self.logic_aborts.add(t_id)
                response = str(response)
                logging.error(f'Application logic error: {response}')
            self.response_buffer[t_id] = (payload.request_id, response)

    async def send_commit_to_peers(self, aborted: set[int]):
        for worker_id, url in self.peers.items():
            await self.scheduler.spawn(self.networking.send_message(url[0], url[1],
                                                                    {"__COM_TYPE__": 'CMT',
                                                                     "__MSG__": (self.id, list(aborted),
                                                                                 list(self.logic_aborts),
                                                                                 self.sequencer.t_counter)},
                                                                    Serializer.MSGPACK))

    async def send_responses(self, committed_t_ids: set):
        for t_id, response in self.response_buffer.items():
            logging.info(f'Committed: {committed_t_ids} logic aborts: {self.logic_aborts}')
            if t_id in committed_t_ids or t_id in self.logic_aborts:
                await self.scheduler.spawn(
                    self.kafka_egress_producer.send_and_wait(EGRESS_TOPIC_NAME,
                                                             key=response[0],
                                                             value=msgpack_serialization(response[1])))

    async def function_scheduler(self):
        while True:
            await asyncio.sleep(EPOCH_INTERVAL)  # EPOCH TIMER
            sequence: set[SequencedItem] = await self.sequencer.get_epoch()  # GET SEQUENCE
            if sequence is not None:
                # Run all the epochs functions concurrently
                epoch_start = timer()
                logging.info(f'Running functions... with sequence: {sequence}')
                run_function_tasks = [asyncio.ensure_future(self.run_function(sequenced_item.t_id,
                                                                              sequenced_item.payload))
                                      for sequenced_item in sequence]
                await asyncio.gather(*run_function_tasks)
                # Wait for chains to finish
                end_proc_time = timer()
                logging.warning(f'Functions finished in: {round((end_proc_time - epoch_start)*1000, 4)}ms')
                logging.info(f'Waiting on chained functions...')
                chain_acks = [ack.wait()
                              for ack in self.networking.waited_ack_events.values()]
                await asyncio.gather(*chain_acks)
                chain_done_time = timer()
                logging.warning(f'Chain proc finished in: {round((chain_done_time - end_proc_time) * 1000, 4)}ms')
                # Check for local state conflicts
                logging.info(f'Checking conflicts...')
                aborted: set[int] = self.local_state.check_conflicts()
                checking_conflicts_time = timer()
                logging.warning(f'Checking conflicts in: {round((checking_conflicts_time - chain_done_time) * 1000, 4)}ms')
                # Notify peers that we are ready to commit
                logging.info(f'Notify peers...')
                commit_start = timer()
                await self.send_commit_to_peers(aborted)
                # Wait for remote to be ready to commit
                wait_remote_commits_task = [event.wait()
                                            for event in self.ready_to_commit_events.values()]
                logging.info(f'Waiting on remote commits...')
                await asyncio.gather(*wait_remote_commits_task)
                # Gather the different abort messages (application logic, concurrency)
                remote_aborted: set[int] = set(
                    itertools.chain.from_iterable(self.aborted_from_remote.values())
                )
                logic_aborts_everywhere: set[int] = set(
                    itertools.chain.from_iterable(self.logic_aborts_from_remote.values())
                )
                logic_aborts_everywhere.union(self.logic_aborts)
                # Commit the local while taking into account the aborts from remote
                logging.info(f'Sequence committed!')
                committed_t_ids = await self.local_state.commit(remote_aborted.union(logic_aborts_everywhere))
                await self.send_responses(committed_t_ids)
                # Cleanup
                await self.sequencer.increment_epoch(self.t_counters.values())
                self.cleanup_after_epoch()
                # Re-sequence the aborted transactions due to concurrency
                await self.sequencer.sequence_aborts_for_next_epoch(aborted.union(remote_aborted))
                epoch_end = timer()
                logging.warning(f'Commit took: {round((epoch_end - commit_start) * 1000, 4)}ms')
                logging.warning(f'Epoch: {self.sequencer.epoch_counter - 1} done in '
                                f'{round((epoch_end - epoch_start)*1000, 4)}ms '
                                f'processed: {len(run_function_tasks)} functions '
                                f'initiated {len(chain_acks)} chains')
            elif self.remote_wants_to_commit():
                epoch_start = timer()
                await self.send_commit_to_peers(set())
                # Wait for remote to be ready to commit
                wait_remote_commits_task = [event.wait()
                                            for event in self.ready_to_commit_events.values()]
                logging.info(f'Waiting on remote commits...')
                await asyncio.gather(*wait_remote_commits_task)
                await self.sequencer.increment_epoch(self.t_counters.values())
                self.cleanup_after_epoch()
                epoch_end = timer()
                logging.warning(f'Epoch: {self.sequencer.epoch_counter - 1} done in '
                                f'{round((epoch_end - epoch_start)*1000, 4)}ms '
                                f'processed 0 functions directly')

    def cleanup_after_epoch(self):
        for event in self.ready_to_commit_events.values():
            event.clear()
        self.aborted_from_remote = {}
        self.logic_aborts = set()
        self.logic_aborts_from_remote = {}
        self.t_counters = {}
        self.networking.cleanup_after_epoch()
        self.response_buffer = {}

    def remote_wants_to_commit(self) -> bool:
        for ready_to_commit_event in self.ready_to_commit_events.values():
            if ready_to_commit_event.is_set():
                return True
        return False

    async def start_kafka_egress_producer(self):
        self.kafka_egress_producer = AIOKafkaProducer(bootstrap_servers=[KAFKA_URL],
                                                      enable_idempotence=True)
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
                        for message in messages:
                            await self.handle_message_from_kafka(message)
        finally:
            await consumer.stop()

    async def handle_message_from_kafka(self, msg):
        logging.info(f"Consumed: {msg.topic} {msg.partition} {msg.offset} "
                     f"{msg.key} {msg.value} {msg.timestamp}")
        deserialized_data: dict = self.networking.decode_message(msg.value)
        message_type: str = deserialized_data['__COM_TYPE__']
        message = deserialized_data['__MSG__']
        if message_type == 'RUN_FUN':
            run_func_payload: RunFuncPayload = self.unpack_run_payload(message, msg.key, timestamp=msg.timestamp)
            logging.info(f'SEQ FROM KAFKA: {run_func_payload.function_name} {run_func_payload.key}')
            await self.sequencer.sequence(run_func_payload)
        else:
            logging.error(f"Invalid message type: {message_type} passed to KAFKA")

    async def worker_controller(self, deserialized_data, resp_adr):
        message_type: str = deserialized_data['__COM_TYPE__']
        message = deserialized_data['__MSG__']
        match message_type:
            case 'RUN_FUN_REMOTE' | 'RUN_FUN_RQ_RS_REMOTE':
                request_id = message['__RQ_ID__']
                if message_type == 'RUN_FUN_REMOTE':
                    logging.info('CALLED RUN FUN FROM PEER')
                    payload = self.unpack_run_payload(message, request_id)
                    await self.scheduler.spawn(self.run_function(message['__T_ID__'],
                                                                 payload,
                                                                 ack_payload=deserialized_data['__ACK__']))
                else:
                    logging.info('CALLED RUN FUN RQ RS FROM PEER')
                    payload = self.unpack_run_payload(message, request_id, resp_adr)
                    await self.scheduler.spawn(self.run_function(message['__T_ID__'],
                                                                 payload))
            case 'RECEIVE_EXE_PLN':  # RECEIVE EXECUTION PLAN OF A DATAFLOW GRAPH
                # This contains all the operators of a job assigned to this worker
                await self.handle_execution_plan(message)
                self.attach_state_to_operators()
            case 'CMT':
                remote_worker_id, aborted, logic_aborts, remote_t_counter = message
                self.ready_to_commit_events[remote_worker_id].set()
                self.aborted_from_remote[remote_worker_id] = set(aborted)
                self.logic_aborts_from_remote[remote_worker_id] = set(logic_aborts)
                self.t_counters[remote_worker_id] = remote_t_counter
            case 'ACK':
                ack_id, fraction_str = message
                if fraction_str != '-1':
                    await self.networking.add_ack_fraction_str(ack_id, fraction_str)
                    logging.info(f'ACK received for {ack_id} part: {fraction_str}')
                else:
                    logging.info(f'ABORT ACK received for {ack_id}')
                    await self.networking.abort_chain(ack_id)
            case _:
                logging.error(f"Worker Service: Non supported command message type: {message_type}")

    def attach_state_to_operators(self):
        operator_names: set[str] = set([operator.name for operator in self.registered_operators.values()])
        if self.operator_state_backend == LocalStateBackend.DICT:
            self.local_state = InMemoryOperatorState(operator_names)
        elif self.operator_state_backend == LocalStateBackend.REDIS:
            self.local_state = RedisOperatorState(operator_names)
        else:
            logging.error(f"Invalid operator state backend type: {self.operator_state_backend}")
            return
        for operator in self.registered_operators.values():
            operator.attach_state_networking(self.local_state, self.networking, self.dns, self.operator_functions)

    async def handle_execution_plan(self, message):
        worker_operators, self.dns, self.peers, self.operator_state_backend, self.operator_functions = message
        self.sequencer.set_n_workers(len(self.peers))
        del self.peers[self.id]
        operator: Operator
        for tup in worker_operators:
            operator, partition = tup
            self.registered_operators[(operator.name, partition)] = operator
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
                     f"IP:{self.networking.host_name}")
        while True:
            resp_adr, data = await self.router.read()
            deserialized_data: dict = self.networking.decode_message(data)
            if '__COM_TYPE__' not in deserialized_data:
                logging.error(f"Deserialized data do not contain a message type")
            else:
                await self.worker_controller(deserialized_data, resp_adr)

    @staticmethod
    def unpack_run_payload(message: dict, request_id: str, timestamp=None, response_socket=None) -> RunFuncPayload:
        timestamp = message['__TIMESTAMP__'] if timestamp is None else timestamp
        return RunFuncPayload(request_id, message['__KEY__'], timestamp,
                              message['__OP_NAME__'], message['__PARTITION__'],
                              message['__FUN_NAME__'], message['__PARAMS__'], response_socket)

    async def register_to_coordinator(self):
        self.id = await self.networking.send_message_request_response(
            DISCOVERY_HOST, DISCOVERY_PORT,
            {"__COM_TYPE__": 'REGISTER_WORKER',
             "__MSG__": self.networking.host_name},
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
