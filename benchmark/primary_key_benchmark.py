import time
from time import time

import dask
import pandas as pd
from dask.diagnostics import ProgressBar

from datatracer import DataTracer, load_datasets


@dask.delayed
def primary_key(solver, target, datasets):
    """Benchmark the primary key solver on the target dataset.

    Args:
        solver: The name of the primary key pipeline.
        target: The name of the target dataset.
        datases: A dictionary mapping dataset names to (metadata, tables) tuples.

    Returns:
        A dictionary mapping metric names to values.
    """
    datasets = datasets.copy()
    metadata, tables = datasets.pop(target)

    tracer = DataTracer(solver)
    tracer.fit(datasets)

    y_true = {}
    for table in metadata.get_tables():
        if "primary_key" not in table:
            y_true[table["name"]] = set()
        elif not isinstance(table["primary_key"], str):
            y_true[table["name"]] = set(table["primary_key"])
        else:
            y_true[table["name"]] = set([table["primary_key"]])

    """
    if len(y_true) == 0:
        return {}  # Skip dataset, no primary keys found.
    """

    correct, total_pred, total_true = 0, 0, 0

    try:
        start = time()
        y_pred = tracer.solve(tables)
        end = time()
    except BaseException:
        return {
            "precision": 0,
            "recall": 0,
            "f1": 0,
            "inference_time": 0,
            "status": "ERROR"
        }
    for table_name, primary_key in y_true.items():
        ans = y_pred.get(table_name)
        if isinstance(ans, str):
            ans = set([ans])
        else:
            ans = set(ans)
        correct += len(ans.intersection(primary_key))
        total_pred += len(ans)
        total_true += len(primary_key)

    if correct == 0 or total_pred == 0 or \
            total_true == 0:
        return {
            "precision": 0.0,
            "recall": 0.0,
            "f1": 0.0,
            "inference_time": end - start,
            "status": "OK"
        }
    precision = correct / total_pred
    recall = correct / total_true
    f1 = 2 * precision * recall / (precision + recall)

    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "inference_time": end - start,
        "status": "OK"
    }


def benchmark_primary_key(data_dir, dataset_name=None, solver="datatracer.primary_key.basic"):
    """Benchmark the primary key solver.

    This uses leave-one-out validation and evaluates the performance of the
    solver on the specified datasets.

    Args:
        data_dir: The directory containing the datasets.
        dataset_name: The target dataset to test on. If none is provided, will test on all available datasets by default.
        solver: The name of the primary key pipeline.

    Returns:
        A DataFrame containing the benchmark resuls.
    """
    datasets = load_datasets(data_dir)
    dataset_names = list(datasets.keys())
    if dataset_name is not None:
        if dataset_name in dataset_names:
            dataset_names = [dataset_name]
        else:
            return None
    datasets = dask.delayed(datasets)
    dataset_to_metrics = {}
    for dataset_name in dataset_names:
        dataset_to_metrics[dataset_name] = primary_key(
            solver=solver, target=dataset_name, datasets=datasets)

    with ProgressBar():
        results = dask.compute(dataset_to_metrics)[0]
    for dataset_name, metrics in results.items():
        metrics["dataset"] = dataset_name
    df = pd.DataFrame(list(results.values()))
    dataset_col = df.pop('dataset')
    df.insert(0, 'dataset', dataset_col)
    return df
