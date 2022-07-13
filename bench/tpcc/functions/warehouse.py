from universalis.common.stateful_function import StatefulFunction


class InitialiseWarehouse(StatefulFunction):
    async def run(self):
        data = {
            'w_id': w_id,
            'w_name': d_id,
            'w_street_1': d_street_1,
            'w_street_2': d_street_2,
            'w_city': d_city,
            'w_state': d_state,
            'w_zip': d_zip,
            'w_tax': d_tax,
            'w_ytd': d_ytd,
        }

