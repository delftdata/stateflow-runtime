import math
import os
import pathlib
import re

import numpy as np
import pandas as pd

from common.logging import logging


def calculate_latency_metrics(raw_results: pd.DataFrame):
    latency_metrics: list[dict[str, dict]] = []

    operations: list[str] = raw_results['function'].drop_duplicates().to_list()
    for operation in operations:
        op_entries = raw_results[raw_results['function'] == operation]['latency']

        latency_metrics += [{
            'operation': operation,
            'min': min(op_entries),
            'mean': np.mean(op_entries),
            'median': np.median(op_entries),
            'std': np.std(op_entries),
            'var': np.var(op_entries),
            'max': max(op_entries),
        }]

    latency_metrics += [{
        'operation': 'Combined',
        'max': max(raw_results['latency']),
        'mean': np.mean(raw_results['latency']),
        'median': np.median(raw_results['latency']),
        'std': np.std(raw_results['latency']),
        'var': np.var(raw_results['latency']),
        'min': min(raw_results['latency']),
    }]

    return latency_metrics


def calculate_throughput_metrics(raw_results: pd.DataFrame):
    start_time = -math.inf
    bucket_id = -1
    max_latency_threshold = np.percentile(raw_results['latency'], 90)
    granularity = 1000  # 1 second (ms) (i.e. bucket size)
    throughput: dict[int, int] = {}

    for index, row in raw_results.iterrows():
        if row['timestamp_y'] - start_time > granularity:
            bucket_id += 1
            start_time = row['timestamp_y']
            throughput[bucket_id] = 1
        elif row['latency'] <= max_latency_threshold:
            throughput[bucket_id] += 1

    tps = list(throughput.values())
    return [{
        'max': max(tps),
        'mean': np.mean(tps),
        'median': np.median(tps),
        'std': np.std(tps),
        'var': np.var(tps),
        'min': min(tps)
    }]


def calculate_abort_rate_metrics():
    worker_abort_rate_files = pathlib.Path('./results')
    worker_metrics = {}
    abort_rate_metrics = {}

    for worker_file in worker_abort_rate_files.glob('abort_rates_worker_[0-9]*.csv'):
        try:
            worker_id = re.findall('\d+', worker_file.stem)[0]
            worker_metrics[worker_id] = pd.read_csv(worker_file)

        except AttributeError:
            logging.info(f'{worker_file} could not be processed')
        finally:
            os.remove(worker_file)

    abort_rates = pd.concat(worker_metrics) \
        .rename_axis(['id', None]) \
        .reset_index(level='id') \
        .rename(columns={'id': 'worker_id'})

    abort_rate_metrics['abort_rate'] = abort_rates['abort_rate'].mean()
    abort_rate_metrics['abort_rate_%'] = abort_rate_metrics['abort_rate'] * 100

    return abort_rate_metrics


def check_for_missed_messages(results: pd.DataFrame):
    missed = results[results['response'].isna()]

    if len(missed) > 0:
        print('--------------------')
        print('\nMISSED MESSAGES!\n')
        print('--------------------')
        print(missed)
        print('--------------------')
    else:
        print('\nNO MISSED MESSAGES!\n')

    return len(missed)
