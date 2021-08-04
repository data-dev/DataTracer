import time
from time import time

import dask
import pandas as pd
from dask.diagnostics import ProgressBar

from datatracer import DataTracer, load_datasets


def transform_single_column(tables, column_info):
    aggregation = column_info['aggregation']
    column_name = column_info['source_col']['col_name']
    fk = column_info['row_map']
    if aggregation:
        transformer = eval(aggregation)
        return transformer(tables, fk, column_name)
    else:
        return tables[column_info['source_col']['table_name']][column_name].fillna(0.0).values


def produce_target_column(tables, map_info):
    transformation = map_info['transformation']
    if transformation:
        transformed_columns = []
        for col_info in map_info['lineage_columns']:
            transformed_columns.append(transform_single_column(tables, col_info))
        transformer = eval(transformation)
        return transformer(transformed_columns)
    else:
        return None


def approx_equal(num, target, add_margin, multi_margin):
    if target >= 0:
        return (num <= target * (1 + multi_margin) + add_margin) and (num >=
                                                                      target * (1 - multi_margin) - add_margin)
    else:
        return (num <= target * (1 - multi_margin) + add_margin) and (num >=
                                                                      target * (1 + multi_margin) - add_margin)


def approx_equal_arrays(num, target, add_margin, multi_margin):
    for n, t in zip(num, target):
        if not approx_equal(n, t, add_margin, multi_margin):
            return False
    return True


@dask.delayed
def evaluate_single_lineage(constraint, tracer, tables):
    field = constraint["fields_under_consideration"][0]
    related_fields = constraint["related_fields"]

    y_true = set()
    for related_field in related_fields:
        y_true.add((related_field["table"], related_field["field"]))

    try:
        start = time()
        ret_dict = tracer.solve(tables, target_table=field["table"], target_field=field["field"])
        y_pred = {(col['source_col']['table_name'], col['source_col']['col_name'])
                  for col in ret_dict['lineage_columns']}
        end = time()
    except BaseException:
        return {
            "table": field["table"],
            "field": field["field"],
            "precision": 0,
            "inference_time": 0,
            "status": "ERROR",
        }

    if len(y_pred) == len(y_true) and \
            len(y_true.intersection(y_pred)) == len(y_pred):
        predicted_target = produce_target_column(tables, ret_dict)
        target_column = tables[field["table"]][field["field"]].fillna(0.0).values
        if approx_equal_arrays(predicted_target, target_column, 1e-8, 1e-8):
            precision = 1
        else:
            precision = 0
    else:
        precision = 0
    return {
        "table": field["table"],
        "field": field["field"],
        "precision": precision,
        "inference_time": end - start,
        "status": "OK",
    }


@dask.delayed
def how_lineage(solver, target, datasets):
    """Benchmark the how lineage solver on the target dataset.

    Args:
        solver: The name of the how lineage pipeline.
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
        list_of_metrics.append(evaluate_single_lineage(constraint, tracer, tables))

    list_of_metrics = dask.compute(list_of_metrics)[0]
    return list_of_metrics


def benchmark_how_lineage(data_dir, dataset_name=None, solver="datatracer.how_lineage.basic"):
    """Benchmark the how lineage solver.

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
    dataset_names = list(datasets.keys())
    if dataset_name is not None:
        if dataset_name in dataset_names:
            dataset_names = [dataset_name]
        else:
            return None
    datasets = dask.delayed(datasets)
    dataset_to_metrics = {}
    for dataset_name in dataset_names:
        dataset_to_metrics[dataset_name] = how_lineage(
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
