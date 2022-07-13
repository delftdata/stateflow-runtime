from universalis.common.stateful_function import StatefulFunction


class InitialiseNewOrders(StatefulFunction):
    async def run(self):
        data = {
            'no_o_id': no_o_id,
            'no_d_id': no_d_id,
            'no_w_id': no_w_id,
        }