import redis.asyncio as redis

from universalis.common.logging import logging
from universalis.common.base_state import BaseOperatorState
from universalis.common.serialization import msgpack_serialization, msgpack_deserialization


class ReadUncommitedException(Exception):
    pass


class RedisOperatorState(BaseOperatorState):

    def __init__(self, db: int = 0):
        super().__init__()
        self.local_redis_instance = redis.Redis(unix_socket_path='/tmp/redis.sock', db=db)

    async def put(self, key, value, t_id: int):
        logging.warning(f'PUT: {key}:{value} with t_id: {t_id}')
        async with self.write_set_lock:
            serialized_value = msgpack_serialization(value)
            if t_id in self.write_sets:
                self.write_sets[t_id][key] = serialized_value
            else:
                self.write_sets[t_id] = {key: serialized_value}
            self.writes[key] = min(self.writes.get(key, t_id), t_id)

    async def get(self, key, t_id: int):
        logging.warning(f'GET: {key} with t_id: {t_id}')
        async with self.read_set_lock:
            if t_id in self.read_sets:
                self.read_sets[t_id].add(key)
            else:
                self.read_sets[t_id] = {key}
        db_value = await self.local_redis_instance.get(key)
        if db_value is None:
            logging.error(f'Read uncommitted or does not exit in DB of key: {key}')
            raise ReadUncommitedException(f'Read uncommitted or does not exit in DB of key: {key}')
        else:
            value = msgpack_deserialization(db_value)
            return value

    async def delete(self, key):
        # Need to find a way to implement deletes
        pass

    async def exists(self, key):
        return True if await self.local_redis_instance.exists(key) > 0 else False

    async def commit(self, aborted_from_remote: set[int]):
        self.aborted_transactions: set[int] = self.aborted_transactions.union(aborted_from_remote)
        updates_to_commit = {}
        if len(self.write_sets) == 0:
            return
        for t_id, ws in self.write_sets.items():
            if t_id not in self.aborted_transactions:
                updates_to_commit.update(ws)
        logging.warning(f'Committing: {updates_to_commit}')
        await self.local_redis_instance.mset(updates_to_commit)
        self.cleanup()
