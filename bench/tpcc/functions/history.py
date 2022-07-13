from universalis.common.stateful_function import StatefulFunction


class InitialiseHistory(StatefulFunction):
    async def run(self):
        data = {
            'h_c_id': h_c_id,
            'h_c_d_id': h_c_d_id,
            'h_c_w_id': h_c_w_id,
            'h_d_id': h_d_id,
            'h_w_id': h_w_id,
            'h_date': h_date,
            'h_amount': h_amount,
            'h_data': h_data,
        }