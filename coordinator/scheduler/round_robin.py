from coordinator.scheduler.base_scheduler import BaseScheduler
from coordinator.stateflow_graph import StateflowGraph
from coordinator.worker import StateflowWorker


class RoundRobin(BaseScheduler):

    def __init__(self):
        self.schedule_plan = {}

    def schedule(self, workers: list[StateflowWorker], execution_graph: StateflowGraph):
        print(workers)
        for operator_name, operator, connections in iter(execution_graph):
            current_worker = workers.pop(0)
            print(operator_name)
            print(operator)
            print(connections)
            print(current_worker)
            print(workers)
            workers.append(current_worker)
            print(workers)
            self.schedule_plan[operator_name] = current_worker
        print(self.schedule_plan)
