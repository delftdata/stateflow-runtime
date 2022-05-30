import asyncio
from abc import abstractmethod, ABC
from typing import Any


class BaseOperatorState(ABC):

    def __init__(self):
        self.write_set_lock = asyncio.Lock()
        self.write_sets: dict[int, dict[Any, Any]] = {}
        self.writes: dict[Any, int] = {}
        self.read_set_lock = asyncio.Lock()
        self.read_sets: dict[int, set[Any]] = {}
        self.aborted_transactions: list[int] = []

    @abstractmethod
    async def put(self, key, value, t_id: int):
        raise NotImplementedError

    @abstractmethod
    async def get(self, key, t_id: int):
        raise NotImplementedError

    @abstractmethod
    async def delete(self, key):
        raise NotImplementedError

    @abstractmethod
    async def exists(self, key):
        raise NotImplementedError

    @abstractmethod
    async def commit(self, aborted_from_remote: list[int]):
        raise NotImplementedError

    def check_conflicts(self) -> list[int]:
        for t_id, ws in self.write_sets.items():
            ws = self.write_sets[t_id]
            rs = self.read_sets.get(t_id, set())
            keys = rs.union(set(ws.keys()))
            for key in keys:
                if key in self.writes and self.writes[key] < t_id:
                    self.aborted_transactions.append(t_id)
        return self.aborted_transactions

    def cleanup(self):
        self.write_sets = {}
        self.writes = {}
        self.read_sets = {}
        self.aborted_transactions = []
