from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_ingress import StateflowIngress
from universalis.common.stateflow_worker import StateflowWorker
from universalis.common import NetworkTCPClient
from universalis.common.logging import logging

from .base_scheduler import BaseScheduler


class RoundRobin(BaseScheduler):

    def __init__(self):
        self.networking = NetworkTCPClient()
        self.schedule_plan: dict[str, StateflowWorker] = {}

    async def schedule(self,
                       workers: list[StateflowWorker],
                       ingress: StateflowIngress,
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
                await self.networking.async_transmit_tcp_no_response(message=(operator_name,
                                                                              partition,
                                                                              operator,
                                                                              dns,
                                                                              self.schedule_plan[operator_partition_name]),
                                                                     com_type="SCHEDULE_OPERATOR")
                await self.networking.async_transmit_tcp_no_response(message=(operator_name,
                                                                              partition,
                                                                              self.schedule_plan[operator_partition_name].host,
                                                                              self.schedule_plan[operator_partition_name].port,
                                                                              ingress,
                                                                              ingress),
                                                                     com_type="REGISTER_OPERATOR_INGRESS")
