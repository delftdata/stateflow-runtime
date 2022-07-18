from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, warehouse: dict[str, int | float | str]):
        await self.put(key, warehouse)
        return key, warehouse
