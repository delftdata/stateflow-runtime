from universalis.common.stateful_function import StatefulFunction


class InitialiseItem(StatefulFunction):
    async def run(self, item: tuple):
        i_id, i_im_id, i_name, i_price, i_data = item

        data = {
            'i_id': i_id,
            'i_im_id': i_im_id,
            'i_name': i_name,
            'i_price': i_price,
            'i_data': i_data,
        }

        await self.put(i_id, data)
        return data
