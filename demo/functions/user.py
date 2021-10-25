from universalis.common.operator import StatefulFunction


class CreateUser(StatefulFunction):
    async def run(self, key: str, name: str):
        await self.state.create(key, {'name': name, 'credit': 0})
        return key


class AddCredit(StatefulFunction):
    async def run(self, key: str, credit: int):
        user_data = await self.state.read(key)
        user_data['credit'] += credit
        await self.state.update(key, user_data)


class SubtractCredit(StatefulFunction):
    async def run(self, key: str, credit: int):
        user_data = await self.state.read(key)
        user_data['credit'] -= credit
        await self.state.update(key, user_data)
