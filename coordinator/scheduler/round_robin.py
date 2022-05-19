from universalis.common.operator import BaseOperator
from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_worker import StateflowWorker

from .base_scheduler import BaseScheduler


class RoundRobin(BaseScheduler):

    @staticmethod
    async def schedule(workers: list[StateflowWorker],
                       execution_graph: StateflowGraph,
                       network_manager) -> dict[str, dict[str, tuple[str, int]]]:
        operator_partition_locations: dict[str, dict[str, tuple[str, int]]] = {}
        worker_assignments: dict[tuple[str, int], list[tuple[BaseOperator, int]]] = {(worker.host, worker.port): []
                                                                                     for worker in workers}
        for operator_name, operator, connections in iter(execution_graph):
            for partition in range(operator.n_partitions):
                current_worker = workers.pop(0)
                worker_assignments[(current_worker.host, current_worker.port)].append((operator, partition))
                if operator_name in operator_partition_locations:
                    operator_partition_locations[operator_name].update({str(partition): (current_worker.host,
                                                                                         current_worker.port)})
                else:
                    operator_partition_locations[operator_name] = {str(partition): (current_worker.host,
                                                                                    current_worker.port)}
                workers.append(current_worker)

        for worker, operator_partitions in worker_assignments.items():
            worker_host, worker_port = worker
            await network_manager.send_message(worker_host,
                                               worker_port,
                                               "",
                                               "",
                                               {"__COM_TYPE__": 'RECEIVE_EXE_PLN',
                                                "__MSG__": operator_partitions})
        return operator_partition_locations
