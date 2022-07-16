from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, district: tuple):
        d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id = district

        data = {
            'd_id': d_id,
            'd_w_id': d_w_id,
            'd_name': d_name,
            'd_street_1': d_street_1,
            'd_street_2': d_street_2,
            'd_city': d_city,
            'd_state': d_state,
            'd_zip': d_zip,
            'd_tax': d_tax,
            'd_ytd': d_ytd,
            'd_next_o_id': d_next_o_id
        }

        await self.put(key, data)
