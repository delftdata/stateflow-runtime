from universalis.common.stateful_function import StatefulFunction


class InsertItem(StatefulFunction):
    async def run(self, key: str, item: dict[str, int | float | str]):
        await self.put(key, item)
        return key, item


class GetInsert(StatefulFunction):
    async def run(self, key: str):
        item = await self.get(key)
        return item
