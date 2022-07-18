from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, order: tuple):
        o_id, o_c_id, o_d_id, o_w_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local = order

        data = {
            'o_id': o_id,
            'o_c_id': o_c_id,
            'o_d_id': o_d_id,
            'o_w_id': o_w_id,
            'o_entry_d': o_entry_d,
            'o_carrier_id': o_carrier_id,
            'o_ol_cnt': o_ol_cnt,
            'o_all_local': o_all_local
        }

        await self.put(key, data)
        return data


class NewOrder(StatefulFunction):
    async def run(self, key: str, params: tuple):
        pass

