from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str):
        await self.put(key, 1)


class Read(StatefulFunction):
    async def run(self, key: str):
        data = await self.get(key)
        return data


class Update(StatefulFunction):
    async def run(self, key: str):
        new_value = await self.get(key)
        new_value += 1

        await self.put(key, new_value)
        return key


class Transfer(StatefulFunction):
    async def run(self, key_a: str, key_b: str):
        value_a = await self.get(key_a)
        value_b = await self.get(key_b)

        value_a -= 1
        value_b += 1

        await self.put(key_a, value_a)
        await self.put(key_b, value_b)

        return key_a, key_b
