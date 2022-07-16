from universalis.common.stateful_function import StatefulFunction


class NotEnoughCredit(Exception):
    pass


class Insert(StatefulFunction):
    async def run(self, key: str):
        await self.put(key, 1)
        # return key


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

        self.call_remote_async(
            function=Update,
            key=key_b,
            params=(key_b,)
        )

        value_a -= 1

        if value_a < 0:
            raise NotEnoughCredit(f'Not enough credit for user: {key_a}')

        await self.put(key_a, value_a)

        return key_a
