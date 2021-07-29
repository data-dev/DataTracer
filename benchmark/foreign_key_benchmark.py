import time
from time import time

import dask
import pandas as pd
from dask.diagnostics import ProgressBar

from datatracer import DataTracer, load_datasets


@dask.delayed
def foreign_key(solver, target, datasets):
    """Benchmark the foreign key solver on the target dataset.

    Args:
        solver: The name of the foreign key pipeline.
        target: The name of the target dataset.
        datasets: A dictionary mapping dataset names to (metadata, tables) tuples.

    Returns:
        A dictionary mapping metric names to values.
    """
    datasets = datasets.copy()
    metadata, tables = datasets.pop(target)

    tracer = DataTracer(solver)
    tracer.fit(datasets)

    y_true = set()
    for fk in metadata.get_foreign_keys():
        if not isinstance(fk["field"], str):
            continue  # Skip composite foreign keys
        y_true.add((fk["table"], fk["field"], fk["ref_table"], fk["ref_field"]))

    try:
        start = time()
        fk_pred = tracer.solve(tables)
        end = time()
    except BaseException:
        return {
            "precision": 0,
            "recall": 0,
            "f1": 0,
            "inference_time": 0,
            "status": "ERROR"
        }

    y_pred = set()
    for fk in fk_pred:
        y_pred.add((fk["table"], fk["field"], fk["ref_table"], fk["ref_field"]))

    if len(y_pred) == 0 or len(y_true) == 0 or \
            len(y_true.intersection(y_pred)) == 0:
        return {
            "precision": 0.0,
            "recall": 0.0,
            "f1": 0.0,
            "inference_time": end - start,
            "status": "OK"
        }

    precision = len(y_true.intersection(y_pred)) / len(y_pred)
    recall = len(y_true.intersection(y_pred)) / len(y_true)
    f1 = 2.0 * precision * recall / (precision + recall)

    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "inference_time": end - start,
        "status": "OK"
    }


def benchmark_foreign_key(data_dir, dataset_name=None, solver="datatracer.foreign_key.standard"):
    """Benchmark the foreign key solver.

    This uses leave-one-out validation and evaluates the performance of the
    solver on the specified datasets.

    Args:
        data_dir: The directory containing the datasets.
        dataset_name: The target dataset to test on. If none is provided, will test on all available datasets by default.
        solver: The name of the foreign key pipeline.

    Returns:
        A DataFrame containing the benchmark resuls.
    """
    datasets = load_datasets(data_dir)
    #datasets = sample_datasets(datasets, max_size=20)
    dataset_names = list(datasets.keys())
    if dataset_name is not None:
        if dataset_name in dataset_names:
            dataset_names = [dataset_name]
        else:
            return None
    datasets = dask.delayed(datasets)
    dataset_to_metrics = {}
    for dataset_name in dataset_names:
        dataset_to_metrics[dataset_name] = foreign_key(
            solver=solver, target=dataset_name, datasets=datasets)

    with ProgressBar():
        results = dask.compute(dataset_to_metrics)[0]
    for dataset_name, metrics in results.items():
        metrics["dataset"] = dataset_name
    df = pd.DataFrame(list(results.values()))
    dataset_col = df.pop('dataset')
    df.insert(0, 'dataset', dataset_col)
    return df
