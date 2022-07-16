from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, stock: tuple):
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

        for i, s_dist in enumerate(s_dists):
            data[f's_dist_{(i + 1)}'] = s_dist

        await self.put(key, data)
        return data
