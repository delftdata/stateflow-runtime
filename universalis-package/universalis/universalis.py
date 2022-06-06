import time
import cloudpickle
from aiokafka import AIOKafkaProducer
from kafka.errors import KafkaConnectionError

from universalis.common.serialization import Serializer
from universalis.common.logging import logging
from universalis.common.networking import NetworkingManager
from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_ingress import IngressTypes
from universalis.common.stateflow_worker import StateflowWorker
from universalis.common.operator import BaseOperator, StatefulFunction
from universalis.common.stateful_function import make_key_hashable


class NotAStateflowGraph(Exception):
    pass


class Universalis:

    def __init__(self,
                 coordinator_adr: str,
                 coordinator_port: int,
                 ingress_type: IngressTypes,
                 tcp_ingress_host: str = None,
                 tcp_ingress_port: int = None,
                 kafka_url: str = None):
        self.coordinator_adr = coordinator_adr
        self.coordinator_port = coordinator_port
        self.networking_manager = NetworkingManager()
        if ingress_type == IngressTypes.TCP:
            self.ingress_that_serves: StateflowWorker = StateflowWorker(tcp_ingress_host, tcp_ingress_port)
        elif ingress_type == IngressTypes.KAFKA:
            self.kafka_url = kafka_url
            self.kafka_producer = None

    async def submit(self, stateflow_graph: StateflowGraph, *modules):
        logging.info(f'Submitting Stateflow graph: {stateflow_graph.name}')
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        for module in modules:
            cloudpickle.register_pickle_by_value(module)
        await self.send_execution_graph(stateflow_graph)
        logging.info(f'Submission of Stateflow graph: {stateflow_graph.name} completed')
        time.sleep(0.05)  # Sleep for 50ms to allow for the graph to setup

    async def send_tcp_event(self,
                             operator: BaseOperator,
                             key,
                             function: StatefulFunction,
                             params: tuple,
                             timestamp: int = None):
        if timestamp is None:
            timestamp = time.time_ns()
        event = {'__OP_NAME__': operator.name,
                 '__KEY__': key,
                 '__FUN_NAME__': function.name,
                 '__PARAMS__': params,
                 '__TIMESTAMP__': timestamp}

        await self.networking_manager.send_message(self.ingress_that_serves.host,
                                                   self.ingress_that_serves.port,
                                                   {"__COM_TYPE__": 'REMOTE_FUN_CALL',
                                                    "__MSG__": event},
                                                   Serializer.MSGPACK)

    async def send_kafka_event(self,
                               operator: BaseOperator,
                               key,
                               function: StatefulFunction,
                               params: tuple,
                               timestamp: int = None):
        if self.kafka_producer is None:
            await self.start_kafka_producer()
        if timestamp is None:
            timestamp = time.time_ns()
        partition: int = int(make_key_hashable(key)) % operator.n_partitions
        event = {'__OP_NAME__': operator.name,
                 '__KEY__': key,
                 '__FUN_NAME__': function.name,
                 '__PARAMS__': params,
                 '__PARTITION__': partition,
                 '__TIMESTAMP__': timestamp}
        await self.kafka_producer.send_and_wait(operator.name,
                                                value=event,
                                                partition=partition)

    async def start_kafka_producer(self):
        self.kafka_producer = AIOKafkaProducer(bootstrap_servers=[self.kafka_url],
                                               value_serializer=lambda event: self.networking_manager.encode_message(
                                                   {"__COM_TYPE__": 'RUN_FUN', "__MSG__": event},
                                                   serializer=Serializer.MSGPACK),
                                               enable_idempotence=True)
        while True:
            try:
                await self.kafka_producer.start()
            except KafkaConnectionError:
                time.sleep(1)
                logging.info("Waiting for Kafka")
                continue
            break

    async def send_execution_graph(self, stateflow_graph: StateflowGraph):
        await self.networking_manager.send_message(self.coordinator_adr,
                                                   self.coordinator_port,
                                                   {"__COM_TYPE__": 'SEND_EXECUTION_GRAPH',
                                                    "__MSG__": stateflow_graph})
