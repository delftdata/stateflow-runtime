from universalis.common.operator import StatefulFunction


class CreateOrder(StatefulFunction):
    async def run(self, key, user_key):
        await self.state.create(key, {'user_key': user_key, 'items': []})
        return key


class AddItem(StatefulFunction):
    async def run(self, order_key, item_key, quantity, cost):
        order_data = await self.state.read(order_key)
        order_data['items'].append({'item_key': item_key, 'quantity': quantity, 'cost': cost})
        await self.state.update(order_key, order_data)


class Checkout(StatefulFunction):
    async def run(self, order_key):
        order_data = await self.state.read(order_key)
        total_cost = 0
        for item in order_data['items']:
            # call stock operator here to subtract stock
            await self.call_remote_function_no_response('stock',
                                                        'SubtractStock',
                                                        {'item_key': item['item_key'],
                                                         'quantity': item['quantity']})
            total_cost += item['quantity'] * item['cost']
        # call user operator to subtract credit
        await self.call_remote_function_no_response('user',
                                                    'SubtractCredit',
                                                    {'user_key': order_data['user_key'],
                                                     'credit': total_cost})
