from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, district: dict[str, int | float | str]):
        await self.put(key, district)
        return key, district


class Get(StatefulFunction):
    async def run(self, key: str):
        district = await self.get(key)
        return district
