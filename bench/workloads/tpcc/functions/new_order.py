from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, new_order: dict[str, int | float | str]):
        await self.put(key, new_order)
        return key, new_order
