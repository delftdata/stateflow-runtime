import aioredis
from universalis.common.logging import logging
from universalis.common.base_state import BaseOperatorState
from universalis.common.serialization import msgpack_serialization, msgpack_deserialization
import redis


class RedisOperatorState(BaseOperatorState):

    def __init__(self, db: int = 0):
        self.local_redis_instance = aioredis.from_url('unix:///tmp/redis.sock', db=db)
        self.no_wait_redis = redis.Redis(unix_socket_path='/tmp/redis.sock', db=db)

    async def put(self, key, value):
        # logging.warning(f"(DB) Input -> {value}")
        await self.local_redis_instance.set(key, msgpack_serialization(value))
        # logging.warning(f"(DB) Written -> {await self.get(key)}")

    def put_no_wait(self, key, value):
        self.no_wait_redis.set(key, msgpack_serialization(value))

    async def get(self, key):
        return msgpack_deserialization(await self.local_redis_instance.get(key))

    async def delete(self, key):
        await self.local_redis_instance.delete([key])

    async def exists(self, key):
        return True if await self.local_redis_instance.exists(key) > 0 else False
