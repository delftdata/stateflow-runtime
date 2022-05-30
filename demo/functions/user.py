from universalis.common.operator import StatefulFunction


class CreateUser(StatefulFunction):
    async def run(self, key: str, name: str):
        await self.put(key, {'name': name, 'credit': 0})
        return key


class AddCredit(StatefulFunction):
    async def run(self, key: str, credit: int):
        user_data = await self.get(key)
        user_data['credit'] += credit
        await self.put(key, user_data)
        return user_data


class SubtractCredit(StatefulFunction):
    async def run(self, key: str, credit: int):
        user_data = await self.get(key)
        user_data['credit'] -= credit
        await self.put(key, user_data)
        return user_data
