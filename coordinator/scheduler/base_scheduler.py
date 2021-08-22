from abc import ABC, abstractmethod

from coordinator.stateflow_graph import StateflowGraph
from coordinator.worker import StateflowWorker


class BaseScheduler(ABC):

    @abstractmethod
    def schedule(self, workers: list[StateflowWorker], execution_graph: StateflowGraph):
        raise NotImplementedError
