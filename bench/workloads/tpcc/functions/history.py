from universalis.common.stateful_function import StatefulFunction


class InsertHistory(StatefulFunction):
    async def run(self, key: str, history: dict[str, int | str | float]):
        await self.put(key, history)
        return key, history


class GetHistory(StatefulFunction):
    async def run(self, key: str):
        history = await self.get(key)
        return history
