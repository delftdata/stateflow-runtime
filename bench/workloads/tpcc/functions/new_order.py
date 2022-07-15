from universalis.common.stateful_function import StatefulFunction


class InitialiseNewOrders(StatefulFunction):
    async def run(self, new_order: tuple):
        no_o_id, no_d_id, no_w_id = new_order

        data = {
            'no_o_id': no_o_id,
            'no_d_id': no_d_id,
            'no_w_id': no_w_id,
        }

        await self.put(no_o_id, data)
        return data
