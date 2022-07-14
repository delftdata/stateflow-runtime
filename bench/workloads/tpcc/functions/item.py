from universalis.common.stateful_function import StatefulFunction


class InitialiseItems(StatefulFunction):
    async def run(self, items: list[tuple]):
        for item in items:
            i_id, i_im_id, i_name, i_price, i_data = item

            data = {
                'i_id': i_id,
                'i_im_id': i_im_id,
                'i_name': i_name,
                'i_price': i_price,
                'i_data': i_data,
            }

            await self.put(i_id, data)
