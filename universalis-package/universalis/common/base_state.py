import asyncio
from abc import abstractmethod, ABC
from typing import Any

from universalis.common.logging import logging


class BaseOperatorState(ABC):
    # Locks that allow for concurrent access to the read and write sets
    read_set_locks: dict[str, asyncio.Lock]  # operator_name: Lock
    write_set_locks: dict[str, asyncio.Lock]  # operator_name: Lock
    # read write sets
    read_sets: dict[str, dict[int, set[Any]]]  # operator_name: {t_id: set(keys)}
    write_sets: dict[str, dict[int, dict[Any, Any]]]  # operator_name: {t_id: {key: value}}
    # the reads and writes with the lowest t_id
    writes: dict[str, dict[Any, int]]  # operator_name: {key: t_id}
    reads: dict[str, dict[Any, int]]  # operator_name: {key: t_id}
    # the transactions that are aborted
    aborted_transactions: set[int]

    def __init__(self, operator_names: set[str]):
        self.operator_names = operator_names
        self.read_set_locks = {operator_name: asyncio.Lock() for operator_name in self.operator_names}
        self.write_set_locks = {operator_name: asyncio.Lock() for operator_name in self.operator_names}
        self.cleanup()

    @abstractmethod
    async def put(self, key, value, t_id: int, operator_name: str):
        logging.info(f'PUT: {key}:{value} with t_id: {t_id} operator: {operator_name}')
        async with self.write_set_locks[operator_name]:
            if t_id in self.write_sets[operator_name]:
                self.write_sets[operator_name][t_id][key] = value
            else:
                self.write_sets[operator_name][t_id] = {key: value}
            self.writes[operator_name][key] = min(self.writes[operator_name].get(key, t_id), t_id)

    @abstractmethod
    async def get(self, key, t_id: int, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def delete(self, key, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def exists(self, key, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def commit(self, aborted_from_remote: set[int]):
        raise NotImplementedError

    @staticmethod
    def has_conflicts(t_id: int, keys: set[Any], reservations: dict[Any, int]):
        for key in keys:
            if key in reservations and reservations[key] < t_id:
                return True
        return False

    def check_conflicts(self) -> set[int]:
        for operator_name, write_set in self.write_sets.items():
            for t_id, ws in write_set.items():
                rs = self.read_sets[operator_name].get(t_id, set())
                read_write_set = rs.union(set(ws.keys()))
                if self.has_conflicts(t_id, read_write_set, self.writes[operator_name]):
                    self.aborted_transactions.add(t_id)
        return self.aborted_transactions

    def check_conflicts_deterministic_reordering(self) -> set[int]:
        for operator_name, write_set in self.write_sets.items():
            for t_id, ws in write_set.items():
                rs_keys = self.read_sets[operator_name].get(t_id, set())
                ws_keys = set(ws.keys())
                waw = self.has_conflicts(t_id, ws_keys, self.writes[operator_name])
                war = self.has_conflicts(t_id, ws_keys, self.reads[operator_name])
                raw = self.has_conflicts(t_id, rs_keys, self.writes[operator_name])
                if waw or (war and raw):
                    self.aborted_transactions.add(t_id)
        return self.aborted_transactions

    def cleanup(self):
        self.write_sets = {operator_name: {} for operator_name in self.operator_names}
        self.writes = {operator_name: {} for operator_name in self.operator_names}
        self.reads = {operator_name: {} for operator_name in self.operator_names}
        self.read_sets = {operator_name: {} for operator_name in self.operator_names}
        self.aborted_transactions = set()
