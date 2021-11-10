import os

from universalis.common.serialization import Serializer
from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_worker import StateflowWorker

from .base_scheduler import BaseScheduler

DISCOVERY_HOST = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT = int(os.environ['DISCOVERY_PORT'])


class RoundRobin(BaseScheduler):

    @staticmethod
    def schedule(workers: list[StateflowWorker], execution_graph: StateflowGraph, network_manager):
        for operator_name, operator, connections in iter(execution_graph):
            for partition in range(operator.partitions):
                current_worker = workers.pop(0)

                if (current_worker.host, current_worker.port) not in network_manager.open_socket_connections:
                    network_manager.create_socket_connection(current_worker.host, current_worker.port)
                network_manager.send_message(current_worker.host,
                                             current_worker.port,
                                             {"__COM_TYPE__": 'RECEIVE_EXE_PLN',
                                              "__MSG__": (operator, partition)})

                if (DISCOVERY_HOST, DISCOVERY_PORT) not in network_manager.open_socket_connections:
                    network_manager.create_socket_connection(DISCOVERY_HOST, DISCOVERY_PORT)
                network_manager.send_message(DISCOVERY_HOST,
                                             DISCOVERY_PORT,
                                             {"__COM_TYPE__": 'REGISTER_OPERATOR_DISCOVERY',
                                              "__MSG__": (operator_name,
                                                          partition,
                                                          current_worker.host,
                                                          current_worker.port)},
                                             Serializer.MSGPACK)

                workers.append(current_worker)
