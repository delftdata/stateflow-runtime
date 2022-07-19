from typing import Any

from universalis.common.logging import logging
from universalis.common.base_state import BaseOperatorState


class InMemoryOperatorState(BaseOperatorState):

    data: dict[str, dict[Any, Any]]

    def __init__(self, operator_names: set[str]):
        super().__init__(operator_names)
        self.data = {}
        for operator_name in operator_names:
            self.data[operator_name] = {}

    async def put(self, key, value, t_id: int, operator_name: str):
        await super().put(key, value, t_id, operator_name)

    async def put_immediate(self, key, value, t_id: int, operator_name: str):
        await super().put_immediate(key, value, t_id, operator_name)

    async def commit_fallback_transaction(self, t_id: int):
        if t_id in self.fallback_commit_buffer:
            for operator_name, kv_pairs in self.fallback_commit_buffer[t_id].items():
                for key, value in kv_pairs.items():
                    async with self.fallback_commit_buffer_locks[operator_name]:
                        self.data[operator_name][key] = value

    async def get(self, key, t_id: int, operator_name: str) -> Any:
        async with self.read_set_locks[operator_name]:
            if t_id in self.read_sets[operator_name]:
                self.read_sets[operator_name][t_id].add(key)
            else:
                self.read_sets[operator_name][t_id] = {key}
        try:
            value = self.data[operator_name][key]
            self.reads[operator_name][key] = min(self.reads[operator_name].get(key, t_id), t_id)
            return value
        except KeyError:
            logging.warning(f'Key: {key} does not exist')

    async def delete(self, key: str, operator_name: str):
        # Need to find a way to implement deletes
        pass

    async def exists(self, key, operator_name: str):
        return True if key in self.data[operator_name] else False

    async def commit(self, aborted_from_remote: set[int]) -> set[int]:
        self.aborted_transactions: set[int] = self.aborted_transactions.union(aborted_from_remote)
        committed_t_ids = set()
        for operator_name in self.write_sets.keys():
            updates_to_commit = {}
            for t_id, ws in self.write_sets[operator_name].items():
                if t_id not in self.aborted_transactions:
                    updates_to_commit.update(ws)
                    committed_t_ids.add(t_id)
            self.data[operator_name].update(updates_to_commit)
        return committed_t_ids
