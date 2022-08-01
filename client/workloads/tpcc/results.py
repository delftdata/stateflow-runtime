import os
import pathlib

import pandas as pd

from common.logging import logging
from common.metrics import (
    check_for_missed_messages,
    calculate_abort_rate_metrics,
    calculate_latency_metrics,
    calculate_throughput_metrics,
)


def calculate(requests: list[dict], responses: list[dict], params):
    logging.info('Calculating metrics')

    workload: str = params['workload']
    num_runs: int = params['num_runs']
    num_concurrent_tasks: int = params['num_concurrent_tasks']
    scale_factor: int = params['scale_factor']
    benchmark_duration: int = params['benchmark_duration']

    folder_name: str = f'{workload}_{scale_factor}sf_{benchmark_duration}sec_{num_concurrent_tasks}th'
    results_dir: str = f'./results/{folder_name}'
    pathlib.Path(results_dir).mkdir(parents=True, exist_ok=True)

    requests_df = pd.DataFrame(
        requests,
        columns=['run_number', 'request_id', 'stage', 'function', 'params', 'timestamp']
    )
    responses_df = pd.DataFrame(responses, columns=['request_id', 'response', 'timestamp'])

    raw_results = pd.merge(requests_df, responses_df, on='request_id', how='outer')
    raw_results['latency'] = raw_results['timestamp_y'] - raw_results['timestamp_x']

    # TODO: Fix this issue with Kafka sending duplicate requests
    if any(raw_results['request_id'].duplicated()):
        logging.warning('Requests have been duplicated, removing duplicates')
        raw_results.drop_duplicates(subset=['request_id'])

    raw_results.to_csv(os.path.join(results_dir, 'raw_results.csv'), index=False)
    check_for_missed_messages(raw_results)

    abort_rate_metrics = calculate_abort_rate_metrics()
    transaction_mix_results = raw_results[raw_results['stage'] == 'transaction_mix']
    params['operation_counts'] = (transaction_mix_results['function'].value_counts() / num_runs).astype(int).to_dict()

    latency_metrics = calculate_latency_metrics(transaction_mix_results)
    throughput_metrics = calculate_throughput_metrics(transaction_mix_results)

    pd.DataFrame([params]).to_csv(os.path.join(results_dir, f'benchmark_parameters.csv'), index=False)
    pd.DataFrame(latency_metrics).to_csv(os.path.join(results_dir, f'latency_metrics.csv'), index=False)
    pd.DataFrame(throughput_metrics).to_csv(os.path.join(results_dir, f'throughput_metrics.csv'), index=False)
    pd.DataFrame([abort_rate_metrics]).to_csv(os.path.join(results_dir, f'abort_rate_metrics.csv'), index=False)

    logging.info('Finished calculating metrics')
