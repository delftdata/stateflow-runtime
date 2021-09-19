from abc import ABC, abstractmethod

from common.stateflow_graph import StateflowGraph
from common.stateflow_ingress import StateflowIngress
from common.stateflow_worker import StateflowWorker


class BaseScheduler(ABC):

    @abstractmethod
    def schedule(self,
                 workers: list[StateflowWorker],
                 ingresses: list[StateflowIngress],
                 execution_graph: StateflowGraph):
        raise NotImplementedError
