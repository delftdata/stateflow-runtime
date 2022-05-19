import redis.asyncio as redis

from universalis.common.base_state import BaseOperatorState
from universalis.common.serialization import msgpack_serialization, msgpack_deserialization


class RedisOperatorState(BaseOperatorState):

    def __init__(self, db: int = 0):
        self.local_redis_instance = redis.Redis(unix_socket_path='/tmp/redis.sock', db=db)

    async def put(self, key, value):
        await self.local_redis_instance.set(key, msgpack_serialization(value))

    async def get(self, key):
        return msgpack_deserialization(await self.local_redis_instance.get(key))

    async def delete(self, key):
        await self.local_redis_instance.delete([key])

    async def exists(self, key):
        return True if await self.local_redis_instance.exists(key) > 0 else False
