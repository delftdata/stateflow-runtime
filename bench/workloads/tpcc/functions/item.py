from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, item: tuple):
        i_id, i_im_id, i_name, i_price, i_data = item

        data = {
            'i_id': i_id,
            'i_im_id': i_im_id,
            'i_name': i_name,
            'i_price': i_price,
            'i_data': i_data,
        }

        await self.put(key, data)
        return data
