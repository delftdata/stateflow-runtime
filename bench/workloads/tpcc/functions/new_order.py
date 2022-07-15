from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, new_order: tuple):
        no_w_id, no_d_id, no_o_id = new_order

        data = {
            'no_o_id': no_o_id,
            'no_d_id': no_d_id,
            'no_w_id': no_w_id,
        }

        await self.put(key, data)
        return data
