import os

import cloudpickle
from universalis.common.networking import async_transmit_tcp_no_response, \
    async_transmit_websocket_no_response_new_connection
from universalis.common.serialization import msgpack_serialization
from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_worker import StateflowWorker

from .base_scheduler import BaseScheduler

DISCOVERY_HOST = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT = int(os.environ['DISCOVERY_PORT'])


class RoundRobin(BaseScheduler):

    @staticmethod
    async def schedule(workers: list[StateflowWorker],
                       execution_graph: StateflowGraph):

        for operator_name, operator, connections in iter(execution_graph):
            for partition in range(operator.partitions):
                current_worker = workers.pop(0)

                await async_transmit_tcp_no_response(current_worker.host,
                                                     current_worker.port,
                                                     (operator, partition),
                                                     com_type='RECEIVE_EXE_PLN')
                await async_transmit_tcp_no_response(DISCOVERY_HOST,
                                                     DISCOVERY_PORT,
                                                     (operator_name,
                                                      partition,
                                                      current_worker.host,
                                                      current_worker.port),
                                                     com_type='REGISTER_OPERATOR_DISCOVERY')

                workers.append(current_worker)

    @staticmethod
    async def schedule_websockets(workers: list[StateflowWorker],
                                  execution_graph: StateflowGraph):

        for operator_name, operator, connections in iter(execution_graph):
            for partition in range(operator.partitions):
                current_worker = workers.pop(0)

                await async_transmit_websocket_no_response_new_connection(current_worker.host,
                                                                          current_worker.port,
                                                                          (operator, partition),
                                                                          com_type='RECEIVE_EXE_PLN')
                await async_transmit_websocket_no_response_new_connection(DISCOVERY_HOST,
                                                                          DISCOVERY_PORT,
                                                                          (operator_name,
                                                                           partition,
                                                                           current_worker.host,
                                                                           current_worker.port),
                                                                          com_type='REGISTER_OPERATOR_DISCOVERY')

                workers.append(current_worker)

    @staticmethod
    def schedule_protocol(workers: list[StateflowWorker], execution_graph: StateflowGraph, network_manager):
        for operator_name, operator, connections in iter(execution_graph):
            for partition in range(operator.partitions):
                current_worker = workers.pop(0)

                if (current_worker.host, current_worker.port) not in network_manager.open_socket_connections:
                    network_manager.create_socket_connection(current_worker.host, current_worker.port)
                network_manager.send_message(current_worker.host,
                                             current_worker.port,
                                             cloudpickle.dumps({"__COM_TYPE__": 'RECEIVE_EXE_PLN',
                                                                "__MSG__": (operator, partition)}))

                if (DISCOVERY_HOST, DISCOVERY_PORT) not in network_manager.open_socket_connections:
                    network_manager.create_socket_connection(DISCOVERY_HOST, DISCOVERY_PORT)
                network_manager.send_message(DISCOVERY_HOST,
                                             DISCOVERY_PORT,
                                             msgpack_serialization({"__COM_TYPE__": 'REGISTER_OPERATOR_DISCOVERY',
                                                                    "__MSG__": (operator_name,
                                                                                partition,
                                                                                current_worker.host,
                                                                                current_worker.port)}))

                workers.append(current_worker)
