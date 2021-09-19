from coordinator.scheduler.base_scheduler import BaseScheduler
from common.stateflow_graph import StateflowGraph
from common.stateflow_ingress import StateflowIngress
from common.stateflow_worker import StateflowWorker
from common.network_client import NetworkTCPClient
from common.logging import logging


class RoundRobin(BaseScheduler):

    def __init__(self):
        self.networking = NetworkTCPClient()
        self.schedule_plan: dict[str, StateflowWorker] = {}

    def schedule(self,
                 workers: list[StateflowWorker],
                 ingresses: list[StateflowIngress],
                 execution_graph: StateflowGraph):

        for operator_name, operator, connections in iter(execution_graph):
            current_worker = workers.pop(0)
            self.schedule_plan[operator_name] = current_worker
            workers.append(current_worker)
        for operator_name, operator, connections in iter(execution_graph):
            dns = {connection: self.schedule_plan[connection] for connection in connections}
            logging.info(f'Sending: {operator_name} \n\t with object: {operator} \n\t '
                         f'and active connections {dns} \n\t to {self.schedule_plan[operator_name]}')
            self.networking.transmit_tcp_no_response((operator_name, operator, dns, self.schedule_plan[operator_name]),
                                                     "SCHEDULE_OPERATOR")
            logging.info(f'Registering: {operator_name} \n\t to ingress: {ingresses[0]}')
            self.networking.transmit_tcp_no_response((operator_name,
                                                     self.schedule_plan[operator_name].host,
                                                     self.schedule_plan[operator_name].port,
                                                      ingresses[0].host,
                                                      ingresses[0].port),
                                                     "REGISTER_OPERATOR_INGRESS")
        return ingresses[0].ext_host, ingresses[0].ext_port
