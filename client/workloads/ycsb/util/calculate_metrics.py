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


def calculate_consistency_metrics(
        verification_results: dict,
        balances: dict[(int, str), dict[str, int]],
        num_operations: int,
        num_missed_messages: int,
):

    for result in verification_results.values():
        key, value = result['response']
        balances[(result['run_number'], str(key))]['received'] = value

    inconsistency_metrics: dict = {'inconsistency_cnt': 0}

    total_expected: int = 0
    total_received: int = 0

    for balance in balances.values():
        total_expected += balance['expected']
        total_received += balance['received']

        if balance['expected'] != balance['received']:
            inconsistency_metrics['inconsistency_cnt'] += 1

    inconsistency_metrics['anomaly_score'] = (total_expected - total_received) / num_operations
    inconsistency_metrics['total_received'] = total_received
    inconsistency_metrics['total_expected'] = total_expected
    inconsistency_metrics['missed_messages'] = num_missed_messages

    return [inconsistency_metrics]


def calculate(requests: list[dict], responses: list[dict], balances, params):
    logging.info('Calculating metrics')

    workload: str = params['workload']
    num_rows: int = params['num_rows']
    num_operations: int = params['num_operations']
    num_concurrent_tasks: int = params['num_concurrent_tasks']
    num_transfer_ops: int = params['operation_mix'][2]
    num_runs: int = params['num_runs']

    folder_name: str = f'{workload}_{num_rows}rw_{num_operations}op_{num_concurrent_tasks}th_{num_transfer_ops}tr'
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
    num_missed_messages = check_for_missed_messages(raw_results)

    verification_results = raw_results[raw_results['stage'] == 'validation'].to_dict('index')
    consistency_metrics = calculate_consistency_metrics(
        verification_results,
        balances,
        num_operations,
        num_missed_messages
    )

    abort_rate_metrics = calculate_abort_rate_metrics()

    transaction_mix_results = raw_results[raw_results['stage'] == 'transaction_mix']
    params['operation_counts'] = (transaction_mix_results['function'].value_counts() / num_runs).astype(int).to_dict()

    latency_metrics = calculate_latency_metrics(transaction_mix_results)
    throughput_metrics = calculate_throughput_metrics(transaction_mix_results)

    pd.DataFrame([params]).to_csv(os.path.join(results_dir, f'benchmark_parameters.csv'), index=False)
    pd.DataFrame(consistency_metrics).to_csv(os.path.join(results_dir, f'consistency_metrics.csv'), index=False)
    pd.DataFrame(latency_metrics).to_csv(os.path.join(results_dir, f'latency_metrics.csv'), index=False)
    pd.DataFrame(throughput_metrics).to_csv(os.path.join(results_dir, f'throughput_metrics.csv'), index=False)
    pd.DataFrame([abort_rate_metrics]).to_csv(os.path.join(results_dir, f'abort_rate_metrics.csv'), index=False)

    logging.info('Finished calculating metrics')
