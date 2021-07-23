import time
from time import ctime, time

import dask
import pandas as pd
from dask.diagnostics import ProgressBar

import datatracer
from datatracer import DataTracer, load_datasets

@dask.delayed
def evaluate_single_column_map(constraint, tracer, tables):
    field = constraint["fields_under_consideration"][0]
    related_fields = constraint["related_fields"]

    y_true = set()
    for related_field in related_fields:
        y_true.add((related_field["table"], related_field["field"]))

    try:
        start = time()
        ret_dict = tracer.solve(tables, target_table=field["table"], target_field=field["field"])
        y_pred = ret_dict
        y_pred = {field for field, score in y_pred.items() if score > 0.0}
        end = time()
    except:
        return {
            "table": field["table"],
            "field": field["field"],
            "precision": 0,
            "recall": 0,
            "f1": 0,
            "inference_time": 0,
            "status": "ERROR",
        }

    if len(y_pred) == 0 or len(y_true) == 0 or \
            len(y_true.intersection(y_pred)) == 0:
        return {
            "table": field["table"],
            "field": field["field"],
            "precision": 0.0,
            "recall": 0.0,
            "f1": 0.0,
            "inference_time": end - start,
            "status": "OK",
        }
    else:
        precision = len(y_true.intersection(y_pred)) / len(y_pred)
        recall = len(y_true.intersection(y_pred)) / len(y_true)
        f1 = 2.0 * precision * recall / (precision + recall)

        return {
            "table": field["table"],
            "field": field["field"],
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "inference_time": end - start,
            "status": "OK",
        }

@dask.delayed
def column_map(solver, target, datasets):
    """Benchmark the column map solver on the target dataset.

    Args:
        solver: The name of the column map pipeline.
        target: The name of the target dataset.
        datases: A dictionary mapping dataset names to (metadata, tables) tuples.

    Returns:
        A list of dictionaries mapping metric names to values for each deived column.
    """
    datasets = datasets.copy()
    metadata, tables = datasets.pop(target)
    if not metadata.data.get("constraints"):
        return {}  # Skip dataset, no constraints found.

    tracer = DataTracer(solver)
    tracer.fit(datasets)

    list_of_metrics = []
    for constraint in metadata.data["constraints"]:
        list_of_metrics.append(evaluate_single_column_map(constraint, tracer, tables))

    list_of_metrics = dask.compute(list_of_metrics)[0]
    return list_of_metrics


def benchmark_column_map(data_dir, dataset_name=None, solver="datatracer.column_map.basic"):
    """Benchmark the column map solver.

    This uses leave-one-out validation and evaluates the performance of the 
    solver on the specified datasets.

    Args:
        data_dir: The directory containing the datasets.
        dataset_name: The target dataset to test on. If none is provided, will test on all available datasets by default.
        solver: The name of the column map pipeline.

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
        dataset_to_metrics[dataset_name] = column_map(
            solver=solver, target=dataset_name, datasets=datasets)

    rows = []
    with ProgressBar():
        results = dask.compute(dataset_to_metrics)[0]
    for dataset_name, list_of_metrics in results.items():
        for metrics in list_of_metrics:
            metrics["dataset"] = dataset_name
            rows.append(metrics)
    df = pd.DataFrame(rows)
    dataset_col = df.pop('dataset')
    table_col = df.pop('table')
    field_col = df.pop('field')
    df.insert(0, 'field', field_col)
    df.insert(0, 'table', table_col)
    df.insert(0, 'dataset', dataset_col)
    return df