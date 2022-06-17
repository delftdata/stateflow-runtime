import logging

from universalis.common.operator import StatefulFunction


class NotEnoughCredit(Exception):
    pass


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
        logging.warning(user_data)
        user_data['credit'] -= credit
        if user_data['credit'] < 0:
            raise NotEnoughCredit(f'Not enough credit: {user_data["credit"]} for user: {key}')
        await self.put(key, user_data)
        return user_data
