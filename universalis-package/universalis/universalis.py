import asyncio
import time
import types
import uuid
from typing import Type

import cloudpickle
from aiokafka import AIOKafkaProducer
from kafka.errors import KafkaConnectionError

from universalis.common.serialization import Serializer, msgpack_serialization
from universalis.common.logging import logging
from universalis.common.networking import NetworkingManager
from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_ingress import IngressTypes
from universalis.common.stateflow_worker import StateflowWorker
from universalis.common.operator import BaseOperator
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
            asyncio.get_event_loop().create_task(self.start_kafka_producer())
            logging.info(f'KAFKA INITIALIZED')

    @staticmethod
    def get_modules(stateflow_graph: StateflowGraph):
        modules = {types.ModuleType(stateflow_graph.__module__)}
        for operator in stateflow_graph.nodes.values():
            modules.add(types.ModuleType(operator.__module__))
            for function in operator.functions.values():
                modules.add(types.ModuleType(function.function_definition.__module__))
        return modules

    async def submit(self, stateflow_graph: StateflowGraph):
        logging.info(f'Submitting Stateflow graph: {stateflow_graph.name}')
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        modules = self.get_modules(stateflow_graph)
        system_module_name = __name__.split('.')[0]
        for module in modules:
            if not module.__name__.startswith(system_module_name):  # exclude system modules
                cloudpickle.register_pickle_by_value(module)
        await self.send_execution_graph(stateflow_graph)
        logging.info(f'Submission of Stateflow graph: {stateflow_graph.name} completed')

    async def send_tcp_event(self,
                             operator: BaseOperator,
                             key,
                             function: Type,
                             params: tuple,
                             timestamp: int = None):
        if timestamp is None:
            timestamp = time.time_ns()
        event = {'__OP_NAME__': operator.name,
                 '__KEY__': key,
                 '__FUN_NAME__': function.__name__,
                 '__PARAMS__': params,
                 '__TIMESTAMP__': timestamp}
        logging.info(event)
        await self.networking_manager.send_message(self.ingress_that_serves.host,
                                                   self.ingress_that_serves.port,
                                                   {"__COM_TYPE__": 'REMOTE_FUN_CALL',
                                                    "__MSG__": event},
                                                   Serializer.MSGPACK)

    async def send_kafka_event(self,
                               operator: BaseOperator,
                               key,
                               function: Type,
                               params: tuple):
        partition: int = make_key_hashable(key) % operator.n_partitions
        event = {'__OP_NAME__': operator.name,
                 '__KEY__': key,
                 '__FUN_NAME__': function.__name__,
                 '__PARAMS__': params,
                 '__PARTITION__': partition}
        request_id = uuid.uuid1().int >> 64
        msg = await self.kafka_producer.send_and_wait(operator.name,
                                                      key=request_id,
                                                      value=event,
                                                      partition=partition)
        return request_id, msg.timestamp

    async def start_kafka_producer(self):
        self.kafka_producer = AIOKafkaProducer(bootstrap_servers=[self.kafka_url],
                                               key_serializer=msgpack_serialization,
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
        logging.info(f'KAFKA PRODUCER STARTED')

    async def send_execution_graph(self, stateflow_graph: StateflowGraph):
        await self.networking_manager.send_message(self.coordinator_adr,
                                                   self.coordinator_port,
                                                   {"__COM_TYPE__": 'SEND_EXECUTION_GRAPH',
                                                    "__MSG__": stateflow_graph})

    async def close(self):
        await self.kafka_producer.stop()
