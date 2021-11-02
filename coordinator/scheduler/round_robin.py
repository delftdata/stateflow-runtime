import os

from universalis.common.networking import async_transmit_tcp_no_response
from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_worker import StateflowWorker

from .base_scheduler import BaseScheduler

DISCOVERY_HOST = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT = int(os.environ['DISCOVERY_PORT'])


class RoundRobin(BaseScheduler):

    async def schedule(self,
                       workers: list[StateflowWorker],
                       ingress: StateflowWorker,
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
