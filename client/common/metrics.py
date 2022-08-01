# import math
# import os
# import pathlib
# import re
#
# import numpy as np
# import pandas as pd
# from common.logging import logging
#
#
# def calculate_consistency_metrics(balances: dict[(int, str), dict[str, int]], num_operations: int):
#     inconsistency_metrics: dict = {'inconsistency_cnt': 0}
#
#     total_expected: int = 0
#     total_received: int = 0
#
#     for balance in balances.values():
#         total_expected += balance['expected']
#         total_received += balance['received']
#
#         if balance['expected'] != balance['received']:
#             inconsistency_metrics['inconsistency_cnt'] += 1
#
#     inconsistency_metrics['anomaly_score'] = (total_expected - total_received) / num_operations
#     inconsistency_metrics['total_received'] = total_received
#     inconsistency_metrics['total_expected'] = total_expected
#
#     return [inconsistency_metrics]
#
#
# def calculate_latency_metrics(raw_results: pd.DataFrame):
#     latency_metrics: list[dict[str, dict]] = []
#
#     operations: list[str] = raw_results['function'].drop_duplicates().to_list()
#     for operation in operations:
#         op_entries = raw_results[raw_results['function'] == operation]['latency']
#
#         latency_metrics += [{
#             'operation': operation,
#             'min': min(op_entries),
#             'mean': np.mean(op_entries),
#             'median': np.median(op_entries),
#             'std': np.std(op_entries),
#             'var': np.var(op_entries),
#             'max': max(op_entries),
#         }]
#
#     latency_metrics += [{
#         'operation': 'Combined',
#         'max': max(raw_results['latency']),
#         'mean': np.mean(raw_results['latency']),
#         'median': np.median(raw_results['latency']),
#         'std': np.std(raw_results['latency']),
#         'var': np.var(raw_results['latency']),
#         'min': min(raw_results['latency']),
#     }]
#
#     return latency_metrics
#
#
# def calculate_throughput_metrics(raw_results: pd.DataFrame):
#     start_time = -math.inf
#     bucket_id = -1
#     max_latency_threshold = np.percentile(raw_results['latency'], 90)
#     granularity = 1000  # 1 second (ms) (i.e. bucket size)
#     throughput: dict[int, int] = {}
#
#     for index, row in raw_results.iterrows():
#         if row['timestamp_y'] - start_time > granularity:
#             bucket_id += 1
#             start_time = row['timestamp_y']
#             throughput[bucket_id] = 1
#         elif row['latency'] <= max_latency_threshold:
#             throughput[bucket_id] += 1
#
#     tps = list(throughput.values())
#     return [{
#         'max': max(tps),
#         'mean': np.mean(tps),
#         'median': np.median(tps),
#         'std': np.std(tps),
#         'var': np.var(tps),
#         'min': min(tps)
#     }]
#
#
# def calculate_abort_rate_metrics(num_operations: int):
#     worker_abort_rate_files = pathlib.Path('./results')
#     abort_rate_metrics = {}
#
#     for worker_file in worker_abort_rate_files.glob('abort_rates_worker_[0-9]*.csv'):
#         try:
#             worker_id = re.findall('\d+', worker_file.stem)[0]
#             abort_rate_metrics[worker_id] = pd.read_csv(worker_file)
#
#         except AttributeError:
#             logging.info(f'{worker_file} could not be processed')
#         finally:
#             os.remove(worker_file)
#
#     abort_rates = pd.concat(abort_rate_metrics) \
#         .rename_axis(['id', None]) \
#         .reset_index(level='id') \
#         .rename(columns={'id': 'worker_id'})
#
#     return abort_rate_metrics
#
#
# def merge_results(requests: list[dict], responses: list[dict]):
#     results_dir = './results'
#     logging.info('Calculating Metrics')
#
#     requests_df = pd.DataFrame(
#         requests,
#         columns=['run_number', 'request_id', 'stage', 'function', 'timestamp']
#     )
#     responses_df = pd.DataFrame(responses, columns=['request_id', 'response', 'timestamp'])
#
#     raw_results = pd.merge(requests_df, responses_df, on='request_id', how='outer')
#     raw_results.to_csv(os.path.join(results_dir, 'raw_results.csv'), index=False)
#
#     raw_results['latency'] = raw_results['timestamp_y'] - raw_results['timestamp_x']
#
#     return raw_results.to_dict()
#
#
# def check_for_missed_messages(results: list[dict]):
#     missed = results[results['response'].isna()]
#
#     if len(missed) > 0:
#         print('--------------------')
#         print('\nMISSED MESSAGES!\n')
#         print('--------------------')
#         print(missed)
#         print('--------------------')
#     else:
#         print('\nNO MISSED MESSAGES!\n')
#
#
# def calculate(requests: list[dict], responses: list[dict], balances, params):
#     results_dir = './results'
#     logging.info('Calculating Metrics')
#
#     requests_df = pd.DataFrame(requests, columns=['run_number', 'request_id', 'stage', 'function', 'timestamp'])
#     responses_df = pd.DataFrame(responses, columns=['request_id', 'response', 'timestamp'])
#
#     raw_results = pd.merge(requests_df, responses_df, on='request_id', how='outer')
#     raw_results.to_csv(os.path.join(results_dir, 'raw_results.csv'), index=False)
#     raw_results['latency'] = raw_results['timestamp_y'] - raw_results['timestamp_x']
#
#     missed = raw_results[raw_results['response'].isna()]
#     if len(missed) > 0:
#         print('--------------------')
#         print('\nMISSED MESSAGES!\n')
#         print('--------------------')
#         print(missed)
#         print('--------------------')
#     else:
#         print('\nNO MISSED MESSAGES!\n')
#
#     workload: str = params['workload']
#     num_rows: int = params['num_rows']
#     num_operations: int = params['num_operations']
#     num_concurrent_tasks: int = params['num_concurrent_tasks']
#     num_transfer_ops: int = params['operation_mix'][2]
#     num_runs: int = params['num_runs']
#
#     folder_name: str = f'{workload}_{num_rows}rw_{num_operations}op_{num_concurrent_tasks}th_{num_transfer_ops}tr'
#     results_dir: str = f'./results/{folder_name}'
#     pathlib.Path(results_dir).mkdir(parents=True, exist_ok=True)
#
#     params['operation_counts'] = (raw_results['function'].value_counts() / num_runs).astype(int).to_dict()
#
#     # consistency_metrics = calculate_consistency_metrics(balances, 10 * 10)
#     # abort_rate_metrics = calculate_abort_rate_metrics(num_operations * num_runs)
#     latency_metrics = calculate_latency_metrics(raw_results)
#     throughput_metrics = calculate_throughput_metrics(raw_results)
#
#     pd.DataFrame([params]).to_csv(
#         os.path.join(results_dir, f'benchmark_parameters.csv'),
#         index=False
#     )
#     # pd.DataFrame(consistency_metrics).to_csv(
#     #     os.path.join(results_dir, f'consistency_metrics.csv'),
#     #     index=False
#     # )
#     pd.DataFrame(latency_metrics).to_csv(
#         os.path.join(results_dir, f'latency_metrics.csv'),
#         index=False
#     )
#     pd.DataFrame(throughput_metrics).to_csv(
#         os.path.join(results_dir, f'throughput_metrics.csv'),
#         index=False
#     )
#     # pd.DataFrame([abort_rate_metrics]).to_csv(
#     #     os.path.join(results_dir, f'abort_rate_metrics.csv'),
#     #     index=False
#     # )
