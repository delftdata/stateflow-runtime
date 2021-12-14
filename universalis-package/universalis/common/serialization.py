from enum import Enum, auto

import msgpack


class Serializer(Enum):
    CLOUDPICKLE = auto()
    MSGPACK = auto()


def msgpack_serialization(serializable_object: object) -> bytes:
    return msgpack.packb(serializable_object)


def msgpack_deserialization(serialized_object: bytes) -> dict:
    return msgpack.unpackb(serialized_object)
