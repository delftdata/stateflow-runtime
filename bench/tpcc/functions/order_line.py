from universalis.common.stateful_function import StatefulFunction


class InitialiseOrderLine(StatefulFunction):
    async def run(self):
        data = {
            'ol_o_id': ol_o_id,
            'ol_d_id': ol_d_id,
            'ol_w_id': ol_w_id,
            'ol_number': ol_number,
            'ol_i_id': ol_i_id,
            'ol_supply_w_id': ol_supply_w_id,
            'ol_delivery_d': ol_delivery_d,
            'ol_quantity': ol_quantity,
            'ol_amount': ol_amount,
            'ol_dist_info': ol_dist_info,
        }