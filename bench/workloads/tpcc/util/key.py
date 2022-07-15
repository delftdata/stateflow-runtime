def tuple_to_composite(composite_key: tuple) -> str:
    return ','.join(str(key) for key in composite_key)


def composite_to_tuple(key: str) -> tuple:
    return tuple(map(int, key.split(',')))
