import os

from universalis.common.networking import async_transmit_tcp_no_response
from universalis.common.stateflow_graph import StateflowGraph
# from universalis.common.stateflow_ingress import StateflowIngress
from universalis.common.stateflow_worker import StateflowWorker
from universalis.common.logging import logging

from .base_scheduler import BaseScheduler

DISCOVERY_HOST = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT = int(os.environ['DISCOVERY_PORT'])


class RoundRobin(BaseScheduler):

    def __init__(self):
        self.schedule_plan: dict[str, StateflowWorker] = {}

    async def schedule(self,
                       workers: list[StateflowWorker],
                       ingress: StateflowWorker,
                       # ingress: StateflowIngress,
                       execution_graph: StateflowGraph):

        for operator_name, operator, connections in iter(execution_graph):
            for partition in range(operator.partitions):
                current_worker = workers.pop(0)
                self.schedule_plan[f"{operator_name}:{partition}"] = current_worker
                workers.append(current_worker)
        for operator_name, operator, connections in iter(execution_graph):
            for partition in range(operator.partitions):
                operator_partition_name = f"{operator_name}:{partition}"
                dns = {connection: self.schedule_plan[operator_partition_name] for connection in connections}
                logging.info(f'Sending: {operator_partition_name} \n\t with object: {operator} \n\t '
                             f'and active connections {dns} \n\t to {self.schedule_plan[operator_partition_name]}')
                await async_transmit_tcp_no_response(self.schedule_plan[operator_partition_name].host,
                                                     self.schedule_plan[operator_partition_name].port,
                                                     (operator, partition, dns),
                                                     com_type='RECEIVE_EXE_PLN')
                await async_transmit_tcp_no_response(DISCOVERY_HOST,
                                                     DISCOVERY_PORT,
                                                     (operator_name,
                                                      partition,
                                                      self.schedule_plan[operator_partition_name].host,
                                                      self.schedule_plan[operator_partition_name].port),
                                                     com_type='REGISTER_OPERATOR_DISCOVERY')
