from demo.functions import stock, user

from universalis.common.operator import StatefulFunction


class CreateOrder(StatefulFunction):
    async def run(self, key, user_key):
        await self.put(key, {'user_key': user_key, 'items': []})
        return key


class AddItem(StatefulFunction):
    async def run(self, order_key, item_key, quantity, cost):
        order_data = await self.get(order_key)
        order_data['items'].append({'item_key': item_key, 'quantity': quantity, 'cost': cost})
        await self.put(order_key, order_data)
        return order_data


class Checkout(StatefulFunction):
    async def run(self, order_key):
        order_data = await self.get(order_key)
        total_cost = 0
        for item in order_data['items']:
            # call stock operator to subtract stock
            self.call_remote_async(operator_name='stock',
                                   function_name=stock.SubtractStock,
                                   key=item['item_key'],
                                   params=(item['item_key'], item['quantity']))

            total_cost += item['quantity'] * item['cost']
            # call user operator to subtract credit
        self.call_remote_async(operator_name='user',
                               function_name=user.SubtractCredit,
                               key=order_data['user_key'],
                               params=(order_data['user_key'], total_cost))
        return order_data
