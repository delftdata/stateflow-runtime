from universalis.common.stateful_function import StatefulFunction


class InitialiseCustomer(StatefulFunction):
    async def run(self):
        data = {
            'c_id': c_id,
            'c_d_id': c_d_id,
            'c_w_id': c_w_id,
            'c_first': c_first,
            'c_middle': c_middle,
            'c_last': c_last,
            'c_street_1': c_street_1,
            'c_street_2': c_street_2,
            'c_city': c_city,
            'c_state': c_state,
            'c_zip': c_zip,
            'c_phone': c_phone,
            'c_since': c_since,
            'c_credit': c_credit,
            'c_credit_lim': c_credit_lim,
            'c_discount': c_discount,
            'c_balance': c_balance,
            'c_ytd_payment': c_ytd_payment,
            'c_payment_cnt': c_payment_cnt,
            'c_delivery_cnt': c_delivery_cnt,
            'c_data': c_data,
        }