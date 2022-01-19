import time
import cloudpickle

from universalis.common.serialization import Serializer
from universalis.common.logging import logging
from universalis.common.networking import NetworkingManager
from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_worker import StateflowWorker
from universalis.common.operator import BaseOperator, StatefulFunction


class NotAStateflowGraph(Exception):
    pass


class Universalis:

    def __init__(self, coordinator_adr: str, coordinator_port: int):
        self.coordinator_adr = coordinator_adr
        self.coordinator_port = coordinator_port
        self.networking_manager = NetworkingManager()
        self.ingress_that_serves: StateflowWorker = StateflowWorker('ingress-load-balancer', 4000)

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
                                                   "",
                                                   "",
                                                   {"__COM_TYPE__": 'REMOTE_FUN_CALL',
                                                    "__MSG__": event},
                                                   Serializer.MSGPACK)
        # logging.info("MESSAGE SENT")

    def send_kafka_event(self,
                         operator: BaseOperator,
                         key,
                         function: StatefulFunction,
                         params: tuple,
                         timestamp: int = None):
        pass

    async def send_execution_graph(self, stateflow_graph: StateflowGraph):
        await self.networking_manager.send_message(self.coordinator_adr,
                                                   self.coordinator_port,
                                                   "",
                                                   "",
                                                   {"__COM_TYPE__": 'SEND_EXECUTION_GRAPH',
                                                    "__MSG__": stateflow_graph})
