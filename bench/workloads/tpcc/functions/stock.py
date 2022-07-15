from universalis.common.stateful_function import StatefulFunction


class InitialiseStock(StatefulFunction):
    async def run(self, stock: tuple):
        s_i_id, s_w_id, s_quantity, s_dists, s_ytd, s_order_cnt, s_remote_cnt, s_data = stock

        data = {
            's_i_id': s_i_id,
            's_w_id': s_w_id,
            's_quantity': s_quantity,
            's_ytd': s_ytd,
            's_order_cnt': s_order_cnt,
            's_remote_cnt': s_remote_cnt,
            's_data': s_data,
        }

        for i, s_dist in s_dists:
            data[f's_dist_{i}'] = s_dist

        await self.put(s_i_id, data)
        return data
