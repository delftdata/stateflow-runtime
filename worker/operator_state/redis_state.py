import redis.asyncio as redis

from universalis.common.logging import logging
from universalis.common.base_state import BaseOperatorState
from universalis.common.serialization import msgpack_serialization, msgpack_deserialization


class RedisOperatorState(BaseOperatorState):

    def __init__(self, db: int = 0):
        super().__init__()
        self.local_redis_instance = redis.Redis(unix_socket_path='/tmp/redis.sock', db=db)

    async def put(self, key, value, **kwargs):
        t_id = kwargs['t_id']
        async with self.write_set_lock:
            serialized_value = msgpack_serialization(value)
            if t_id in self.write_sets:
                self.write_sets[t_id][key] = serialized_value
            else:
                self.write_sets[t_id] = {key: serialized_value}
            self.writes[key] = min(self.writes.get(key, t_id), t_id)

    async def get(self, key, **kwargs):
        t_id = kwargs['t_id']
        async with self.read_set_lock:
            if t_id in self.read_sets:
                self.read_sets[t_id].add(key)
            else:
                self.read_sets[t_id] = {key}
        try:
            value = msgpack_deserialization(await self.local_redis_instance.get(key))
            return value
        except KeyError:
            logging.warning(f'Key: {key} does not exist')

    async def delete(self, key):
        # Need to find a way to implement deletes
        pass

    async def exists(self, key):
        return True if await self.local_redis_instance.exists(key) > 0 else False

    async def commit(self):
        updates_to_commit = {}
        for t_id, ws in self.write_sets.items():
            if not self.has_conflicts(t_id):
                updates_to_commit.update(ws)
        await self.local_redis_instance.mset(updates_to_commit)
        aborted = self.aborted_transactions
        self.cleanup()
        return aborted
