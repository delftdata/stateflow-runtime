from enum import Enum, auto
from typing import Any

import msgpack
import cloudpickle
import pickle


class Serializer(Enum):
    CLOUDPICKLE = auto()
    MSGPACK = auto()
    PICKLE = auto()


def msgpack_serialization(serializable_object: object) -> bytes:
    return msgpack.packb(serializable_object)


def msgpack_deserialization(serialized_object: bytes) -> dict | Any:
    return msgpack.unpackb(serialized_object)


def cloudpickle_serialization(serializable_object: object) -> bytes:
    return cloudpickle.dumps(serializable_object)


def cloudpickle_deserialization(serialized_object: bytes) -> dict | Any:
    return cloudpickle.loads(serialized_object)


def pickle_serialization(serializable_object: object) -> bytes:
    return pickle.dumps(serializable_object)


def pickle_deserialization(serialized_object: bytes) -> dict | Any:
    return pickle.loads(serialized_object)
