from universalis.common.stateful_function import StatefulFunction


class InitialiseStock(StatefulFunction):
    async def run(self):
        data = {
            's_i_id': s_i_id,
            's_w_id': s_w_id,
            's_quantity': s_quantity,
            's_dist_01': s_dist_01,
            's_dist_02': s_dist_02,
            's_dist_03': s_dist_03,
            's_dist_04': s_dist_04,
            's_dist_05': s_dist_05,
            's_dist_06': s_dist_06,
            's_dist_07': s_dist_07,
            's_dist_08': s_dist_08,
            's_dist_09': s_dist_09,
            's_dist_10': s_dist_10,
            's_ytd': s_ytd,
            's_order_cnt': s_order_cnt,
            's_remote_cnt': s_remote_cnt,
            's_data': s_data,
        }