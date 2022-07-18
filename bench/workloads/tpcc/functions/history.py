from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, history: tuple):
        h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data = history

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

        await self.put(key, data)
        return data
