import asyncio
import itertools
import os
import time
from timeit import default_timer as timer
from typing import Any

import aiojobs
import aiozmq
import uvloop
import zmq
import pandas as pd

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


SERVER_PORT: int = 8888
DISCOVERY_HOST: str = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT: int = int(os.environ['DISCOVERY_PORT'])
KAFKA_URL: str = os.getenv('KAFKA_URL', None)
INGRESS_TYPE = os.getenv('INGRESS_TYPE', None)
EGRESS_TOPIC_NAME: str = 'universalis-egress'
EPOCH_INTERVAL: float = 0.01  # 10ms
SEQUENCE_MAX_SIZE: int = 100
DETERMINISTIC_REORDERING: bool = True
FALLBACK_STRATEGY_PERCENTAGE: float = 0.1  # if more than 10% aborts use fallback strategy
ABORT_RATE_OUTPUT_INTERVAL = 4 # Intervals between abort rate measurements


class Worker:

    def __init__(self):
        self.id: int = -1
        self.networking = NetworkingManager()
        self.sequencer = Sequencer(SEQUENCE_MAX_SIZE)
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
        self.aborted_from_remote: dict[int, set[int]] = {}  # worker_id: set of aborted t_ids
        self.processed_seq_size: dict[int, int] = {}  # worker_id: size of the remote processed sequence
        self.logic_aborts: set[int] = set()
        self.logic_aborts_from_remote: dict[int, set[int]] = {}
        self.local_state: InMemoryOperatorState | RedisOperatorState | Stateless = Stateless()
        self.t_counters: dict[int, int] = {}
        self.response_buffer: dict[int, tuple] = {}  # t_id: (request_id, response)
        self.operator_functions: dict[str, str] = {}  # function_name: operator_name
        self.abort_rates: dict[int, float] = {} # epoch_number: abort rate

    async def run_function(self,
                           t_id: int,
                           payload: RunFuncPayload,
                           ack_payload=None,
                           wait_completion_events: list[asyncio.Event] = None):
        operator_partition = self.registered_operators[(payload.operator_name, payload.partition)]
        response = await operator_partition.run_function(t_id,
                                                         payload.request_id,
                                                         payload.timestamp,
                                                         payload.function_name,
                                                         ack_payload,
                                                         wait_completion_events,
                                                         *payload.params)
        if wait_completion_events is not None:
            [ev.set() for ev in wait_completion_events]

        if payload.response_socket is not None:
            self.router.write((payload.response_socket, self.networking.encode_message(response,
                                                                                       Serializer.MSGPACK)))
        elif response is not None:
            if isinstance(response, Exception):
                self.logic_aborts.add(t_id)
                response = str(response)
            self.response_buffer[t_id] = (payload.request_id, response)

    async def send_commit_to_peers(self, aborted: set[int], processed_seq_size: int):
        for worker_id, url in self.peers.items():
            await self.scheduler.spawn(self.networking.send_message(url[0], url[1],
                                                                    {"__COM_TYPE__": 'CMT',
                                                                     "__MSG__": (self.id, list(aborted),
                                                                                 list(self.logic_aborts),
                                                                                 self.sequencer.t_counter,
                                                                                 processed_seq_size)},
                                                                    Serializer.MSGPACK))

    async def send_responses(self, concurrency_aborts_everywhere: set, logic_aborts_everywhere: set):
        for t_id, response in self.response_buffer.items():
            if t_id not in concurrency_aborts_everywhere or t_id in logic_aborts_everywhere:
                # if not aborted due to concurrency or aborted based on logic
                await self.scheduler.spawn(
                    self.kafka_egress_producer.send_and_wait(EGRESS_TOPIC_NAME,
                                                             key=response[0],
                                                             value=msgpack_serialization(response[1])))

    async def run_fallback_function(self,
                                    sequenced_item: SequencedItem,
                                    waiting_on_transactions: dict[int, asyncio.Event],
                                    wait_completion_events: list[asyncio.Event]):
        waiting_on_transactions_tasks = [dependency.wait()
                                         for dependency in waiting_on_transactions.values()]
        # Wait for all transactions that this transaction depends on to finish
        await asyncio.gather(*waiting_on_transactions_tasks)
        # Run transaction
        await self.run_function(sequenced_item.t_id,
                                sequenced_item.payload,
                                wait_completion_events=wait_completion_events)

    async def run_fallback_strategy(self, aborts_for_next_epoch: set[int]):
        logging.info('Too many concurrency aborts! Starting fallback strategy...')
        aborted_sequence: set[SequencedItem] = await self.sequencer.get_aborted_sequence(aborts_for_next_epoch)
        # t_id: {operator: keys}
        t_dependencies: dict[int, dict[str, set[Any]]] = self.local_state.get_dependency_graph(aborts_for_next_epoch)
        # (operator, key): {t_id depends on: completion event} sorted by lowest t_id
        waiting_on_transactions: dict[int, dict[int, asyncio.Event]] = {}
        # t_id: list of all the dependant transaction events
        waiting_on_transactions_index: dict[int, list[asyncio.Event]] = {}
        # (operator, key): set of transactions that depend on the key
        dibs: dict[tuple[str, Any], set[int]] = {}
        for t_id, dependencies in t_dependencies.items():
            for operator_name, keys in dependencies.items():
                for key in keys:
                    key_id = (operator_name, key)
                    if key_id in dibs:
                        for dep_t_id in dibs[key_id]:
                            event = asyncio.Event()
                            if t_id in waiting_on_transactions:
                                waiting_on_transactions[t_id].update({dep_t_id: event})
                            else:
                                waiting_on_transactions[t_id] = {dep_t_id: event}
                            if dep_t_id in waiting_on_transactions_index:
                                waiting_on_transactions_index[dep_t_id].append(event)
                            else:
                                waiting_on_transactions_index[dep_t_id] = [event]
                        dibs[key_id].add(t_id)
                    else:
                        dibs[key_id] = {t_id}
        fallback_tasks = []
        # DIBS DONE
        for sequenced_item in aborted_sequence:
            sequenced_function_is_waiting_on = waiting_on_transactions.get(sequenced_item.t_id, {})
            wait_completion_events = waiting_on_transactions_index.get(sequenced_item.t_id, [])
            fallback_tasks.append(self.run_fallback_function(sequenced_item,
                                                             sequenced_function_is_waiting_on,
                                                             wait_completion_events))
        await asyncio.gather(*fallback_tasks)
        logging.info('Fallback strategy completed!')

    async def function_scheduler(self):
        last_measured = timer()

        while True:
            await asyncio.sleep(EPOCH_INTERVAL)  # EPOCH TIMER
            sequence: list[SequencedItem] = await self.sequencer.get_epoch()  # GET SEQUENCE
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
                logging.info(f'Functions finished in: {round((end_proc_time - epoch_start)*1000, 4)}ms')
                logging.info(f'Waiting on chained functions...')
                chain_acks = [ack.wait()
                              for ack in self.networking.waited_ack_events.values()]
                await asyncio.gather(*chain_acks)
                chain_done_time = timer()
                logging.info(f'Chain proc finished in: {round((chain_done_time - end_proc_time) * 1000, 4)}ms')

                # Check for local state conflicts
                logging.info(f'Checking conflicts...')
                if DETERMINISTIC_REORDERING:
                    aborted: set[int] = self.local_state.check_conflicts_deterministic_reordering()
                else:
                    aborted: set[int] = self.local_state.check_conflicts()
                checking_conflicts_time = timer()
                logging.info(f'Checking conflicts in: {round((checking_conflicts_time - chain_done_time) * 1000, 4)}ms')

                # Notify peers that we are ready to commit
                logging.info(f'Notify peers...')
                commit_start = timer()

                await self.send_commit_to_peers(aborted, len(sequence))

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
                logic_aborts_everywhere = logic_aborts_everywhere.union(self.logic_aborts)

                # Commit the local while taking into account the aborts from remote
                logging.info(f'Sequence committed!')
                await self.local_state.commit(remote_aborted.union(logic_aborts_everywhere))
                aborts_for_next_epoch: set[int] = aborted.union(remote_aborted)
                total_processed_functions: int = sum([self.processed_seq_size[worker_id]
                                                      for worker_id in self.processed_seq_size.keys()]) + len(sequence)

                abort_rate: float = len(aborts_for_next_epoch) / total_processed_functions
                self.abort_rates[self.sequencer.epoch_counter-1] = abort_rate
                if abort_rate > FALLBACK_STRATEGY_PERCENTAGE:
                    await self.run_fallback_strategy(aborts_for_next_epoch)
                    aborts_for_next_epoch = set()
                    abort_rate = 0

                await self.send_responses(aborts_for_next_epoch, logic_aborts_everywhere)
                # Cleanup
                await self.sequencer.increment_epoch(self.t_counters.values(), aborts_for_next_epoch)
                # Re-sequence the aborted transactions due to concurrency
                self.cleanup_after_epoch()
                epoch_end = timer()
                logging.info(f'Commit took: {round((epoch_end - commit_start) * 1000, 4)}ms')
                logging.warning(f'Epoch: {self.sequencer.epoch_counter - 1} done in '
                                f'{round((epoch_end - epoch_start)*1000, 4)}ms '
                                f'processed: {len(run_function_tasks)} functions '
                                f'initiated {len(chain_acks)} chains '
                                f'abort rate: {abort_rate}')
            elif self.remote_wants_to_commit():
                epoch_start = timer()
                await self.send_commit_to_peers(set(), 0)
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

            if self.abort_rates and (timer() - last_measured) > ABORT_RATE_OUTPUT_INTERVAL:
                df = pd.DataFrame(self.abort_rates.items(), columns=['epoch_number', 'abort_rate'])
                df.to_csv(f'/usr/local/universalis/bench/abort_rates_worker_{self.id}.csv')
                last_measured = timer()

    def cleanup_after_epoch(self):
        for event in self.ready_to_commit_events.values():
            event.clear()
        self.aborted_from_remote = {}
        self.logic_aborts = set()
        self.logic_aborts_from_remote = {}
        self.t_counters = {}
        self.networking.cleanup_after_epoch()
        self.response_buffer = {}
        self.local_state.cleanup()

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
                remote_worker_id, aborted, logic_aborts, remote_t_counter, processed_seq_size = message
                self.ready_to_commit_events[remote_worker_id].set()
                self.aborted_from_remote[remote_worker_id] = set(aborted)
                self.processed_seq_size[remote_worker_id] = processed_seq_size
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
