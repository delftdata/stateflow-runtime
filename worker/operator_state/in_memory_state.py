from typing import Any

from universalis.common.logging import logging
from universalis.common.base_state import BaseOperatorState


class InMemoryOperatorState(BaseOperatorState):

    def __init__(self):
        super().__init__()
        self.data: dict = {}

    async def put(self, key, value, t_id: int):
        async with self.write_set_lock:
            if t_id in self.write_sets:
                self.write_sets[t_id][key] = value
            else:
                self.write_sets[t_id] = {key: value}
            self.writes[key] = min(self.writes.get(key, t_id), t_id)

    async def get(self, key, t_id: int) -> Any:
        async with self.read_set_lock:
            if t_id in self.read_sets:
                self.read_sets[t_id].add(key)
            else:
                self.read_sets[t_id] = {key}
        try:
            value = self.data[key]
            return value
        except KeyError:
            logging.warning(f'Key: {key} does not exist')

    async def delete(self, key: str):
        # Need to find a way to implement deletes
        pass

    async def exists(self, key):
        return True if key in self.data else False

    async def commit(self, aborted_from_remote: list[int]):
        self.aborted_transactions += aborted_from_remote
        updates_to_commit = {}
        for t_id, ws in self.write_sets.items():
            if t_id not in self.aborted_transactions:
                updates_to_commit.update(ws)
        self.data.update(updates_to_commit)
        self.cleanup()
