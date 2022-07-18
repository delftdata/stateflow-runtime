from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, warehouse: tuple):
        w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd = warehouse

        data = {
            'w_id': w_id,
            'w_name': w_name,
            'w_street_1': w_street_1,
            'w_street_2': w_street_2,
            'w_city': w_city,
            'w_state': w_state,
            'w_zip': w_zip,
            'w_tax': w_tax,
            'w_ytd': w_ytd,
        }

        await self.put(key, data)
        return data
