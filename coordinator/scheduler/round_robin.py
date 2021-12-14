from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_worker import StateflowWorker

from .base_scheduler import BaseScheduler


class RoundRobin(BaseScheduler):

    @staticmethod
    def schedule(workers: list[StateflowWorker],
                 execution_graph: StateflowGraph,
                 network_manager) -> dict[dict[str, tuple[str, int]]]:
        operator_partition_locations: dict[dict[str, tuple[str, int]]] = {}

        for operator_name, operator, connections in iter(execution_graph):
            for partition in range(operator.partitions):
                current_worker = workers.pop(0)

                network_manager.send_message(current_worker.host,
                                             current_worker.port,
                                             "",
                                             "",
                                             {"__COM_TYPE__": 'RECEIVE_EXE_PLN',
                                              "__MSG__": (operator, partition)})

                if operator_name in operator_partition_locations:
                    operator_partition_locations[operator_name].update({str(partition): (current_worker.host,
                                                                                         current_worker.port)})
                else:
                    operator_partition_locations[operator_name] = {str(partition): (current_worker.host,
                                                                                    current_worker.port)}

                workers.append(current_worker)

        return operator_partition_locations
