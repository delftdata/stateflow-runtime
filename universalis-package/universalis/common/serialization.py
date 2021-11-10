from enum import Enum, auto

import msgpack


class Serializer(Enum):
    CLOUDPICKLE = auto()
    MSGPACK = auto()


def msgpack_serialization(serializable_object: object) -> bytes:
    return msgpack.packb(serializable_object)


def msgpack_deserialization(serialized_object: bytes) -> dict:
    return msgpack.unpackb(serialized_object)


if __name__ == '__main__':
    raw = b'\x82\xac__COM_TYPE__\xa7RUN_FUN\xa7__MSG__\x85\xab__OP_NAME__\xa5stock\xac__FUN_NAME__\xadSubtractStock\xad__PARTITION__\x00\xad__TIMESTAMP__\xcf\x16\xb6,G\xcd\x82\xf9\xfc\xaa__PARAMS__\x92\xd9$0d4e3ee1-9a22-498d-8e73-1f7397927d8d\x01\x00\x01\x00\x00\x00\xa5\x82\xac__COM_TYPE__\xa7RUN_FUN\xa7__MSG__\x85\xab__OP_NAME__\xa4user\xac__FUN_NAME__\xaeSubtractCredit\xad__PARTITION__\x00\xad__TIMESTAMP__\xcf\x16\xb6,G\xcd\x82\xf9\xfc\xaa__PARAMS__\x92\xd9$7ab88469-f08e-4fea-913f-73f7112b199c\x01'
    single_msg =b'\x82\xac__COM_TYPE__\xa7RUN_FUN\xa7__MSG__\x85\xab__OP_NAME__\xa5stock\xac__FUN_NAME__\xadSubtractStock\xad__PARTITION__\x00\xad__TIMESTAMP__\xcf\x16\xb6,G\xcd\x82\xf9\xfc\xaa__PARAMS__\x92\xd9$0d4e3ee1-9a22-498d-8e73-1f7397927d8d\x01'
    print(msgpack_deserialization(single_msg))
