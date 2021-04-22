import argparse
import os
import queue
import threading
import time
from io import BytesIO
from time import ctime, time
from urllib.parse import urljoin
from urllib.request import urlopen
from zipfile import ZipFile

import boto3
import dask
import pandas as pd
from dask.diagnostics import ProgressBar

from datatracer import DataTracer, load_datasets, sample_datasets

BUCKET_NAME = 'tracer-data'
DATA_URL = 'http://{}.s3.amazonaws.com/'.format(BUCKET_NAME)


def download(data_dir):
    """Download benchmark datasets from S3.

    This downloads the benchmark datasets from S3 into the target folder in an 
    uncompressed format. It skips datasets that have already been downloaded.

    Args:
        data_dir: The directory to download the datasets to.

    Returns:
        A DataFrame describing the downloaded datasets.

    Raises:
        NoCredentialsError: If AWS S3 credentials are not found.
    """
    rows = []
    client = boto3.client('s3')
    for dataset in client.list_objects(Bucket=BUCKET_NAME)['Contents']:
        if not '.zip' in dataset['Key']:
            continue
        rows.append(dataset)
        dataset_name = dataset['Key'].replace(".zip", "")
        dataset_path = os.path.join(data_dir, dataset_name)
        if os.path.exists(dataset_path):
            dataset["Status"] = "Skipped"
            print("Skipping %s" % dataset_name)
        else:
            dataset["Status"] = "Downloaded"
            print("Downloading %s" % dataset_name)
            with urlopen(urljoin(DATA_URL, dataset['Key'])) as fp:
                with ZipFile(BytesIO(fp.read())) as zipfile:
                    zipfile.extractall(dataset_path)
    return pd.DataFrame(rows)


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
    start = time()
    y_pred = tracer.solve(tables)
    end = time()
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
            "inference_time": end - start
        }
    precision = correct / total_pred
    recall = correct / total_true
    f1 = 2 * precision * recall / (precision + recall)

    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "inference_time": end - start
    }


def benchmark_primary_key(data_dir, solver="datatracer.primary_key.basic"):
    """Benchmark the primary key solver.

    This uses leave-one-out validation and evaluates the performance of the 
    solver on the specified datasets.

    Args:
        data_dir: The directory containing the datasets.
        solver: The name of the primary key pipeline.

    Returns:
        A DataFrame containing the benchmark resuls.
    """
    datasets = load_datasets(data_dir)
    dataset_names = list(datasets.keys())
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

    start = time()
    fk_pred = tracer.solve(tables)
    end = time()

    y_pred = set()
    for fk in fk_pred:
        y_pred.add((fk["table"], fk["field"], fk["ref_table"], fk["ref_field"]))

    if len(y_pred) == 0 or len(y_true) == 0 or \
            len(y_true.intersection(y_pred)) == 0:
        return {
            "precision": 0.0,
            "recall": 0.0,
            "f1": 0.0,
            "inference_time": end - start
        }

    precision = 0.0 if len(y_pred) == 0 else len(y_true.intersection(y_pred)) / len(y_pred)
    recall = 0.0 if len(y_true) == 0 else len(y_true.intersection(y_pred)) / len(y_true)
    f1 = 0.0 if precision + recall == 0 else 2.0 * precision * recall / (precision + recall)

    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "inference_time": end - start
    }


def benchmark_foreign_key(data_dir, solver="datatracer.foreign_key.standard"):
    """Benchmark the foreign key solver.

    This uses leave-one-out validation and evaluates the performance of the 
    solver on the specified datasets.

    Args:
        data_dir: The directory containing the datasets.
        solver: The name of the foreign key pipeline.

    Returns:
        A DataFrame containing the benchmark resuls.
    """
    datasets = load_datasets(data_dir)
    datasets = sample_datasets(datasets, max_size=1000)
    dataset_names = list(datasets.keys())
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
        field = constraint["fields_under_consideration"][0]
        related_fields = constraint["related_fields"]

        y_true = set()
        for related_field in related_fields:
            y_true.add((related_field["table"], related_field["field"]))

        start = time()
        y_pred = tracer.solve(tables, target_table=field["table"], target_field=field["field"])
        end = time()

        y_pred_total = 0
        intersection_total = 0

        for field_temp, score in y_pred.items():
            if field_temp in y_true:
                intersection_total += max(0, min(1, score))
            y_pred_total += max(0, min(1, score))

        #precision = intersection_total / y_pred_total
        #recall = intersection_total / len(y_true)
        #f1 = 2.0 * precision * recall / (precision + recall)

        y_pred = {field for field, score in y_pred.items() if score > 0.0}
        precision = 0.0 if len(y_pred) == 0 else len(y_true.intersection(y_pred)) / len(y_pred)
        recall = 0.0 if len(y_true) == 0 else len(y_true.intersection(y_pred)) / len(y_true)
        f1 = 0.0 if precision + recall == 0 else 2.0 * precision * recall / (precision + recall)

        list_of_metrics.append({
            "table": field["table"],
            "field": field["field"],
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "inference_time": end - start
        })

    return list_of_metrics


def benchmark_column_map(data_dir, solver="datatracer.column_map.basic"):
    """Benchmark the column map solver.

    This uses leave-one-out validation and evaluates the performance of the 
    solver on the specified datasets.

    Args:
        data_dir: The directory containing the datasets.
        solver: The name of the column map pipeline.

    Returns:
        A DataFrame containing the benchmark resuls.
    """
    datasets = load_datasets(data_dir)
    datasets = sample_datasets(datasets, max_size=100)
    dataset_names = list(datasets.keys())
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


def _get_parser():
    shared_args = argparse.ArgumentParser(add_help=False)
    shared_args.add_argument('--data_dir', type=str, 
        default=os.path.expanduser("~/tracer_data"), required=False, 
        help='Path to the benchmark datasets.')
    default_csv = "report_" + ctime().replace(" ", "_") + ".csv"
    default_csv = default_csv.replace(":", "_")
    shared_args.add_argument('--csv', type=str,
        default=os.path.expanduser(default_csv), required=False, 
        help='Path to the CSV file where the report will be written.')

    parser = argparse.ArgumentParser(
        prog='datatracer-benchmark',
        description='DataTracer Benchmark CLI'
    )

    command = parser.add_subparsers(title='command', help='Command to execute')
    parser.set_defaults(benchmark=None)

    subparser = command.add_parser(
        'download',
        parents=[shared_args],
        help='Download datasets from S3.'
    )
    subparser.set_defaults(command=download)

    subparser = command.add_parser(
        'primary',
        parents=[shared_args],
        help='Primary key benchmark.'
    )
    subparser.set_defaults(command=benchmark_primary_key)

    subparser = command.add_parser(
        'foreign',
        parents=[shared_args],
        help='Foreign key benchmark.'
    )
    subparser.set_defaults(command=benchmark_foreign_key)

    subparser = command.add_parser(
        'column',
        parents=[shared_args],
        help='Column map benchmark.'
    )
    subparser.set_defaults(command=benchmark_column_map)

    return parser


def main():
    parser = _get_parser()
    args = parser.parse_args()
    df = args.command(args.data_dir)
    cmd_str = { benchmark_column_map: 'ColMap_',
    benchmark_foreign_key: 'ForeignKey_',
    benchmark_primary_key: 'PrimaryKey_'
    }
    if args.csv and args.command in cmd_str:
        df.to_csv("Reports/" + cmd_str[args.command] + args.csv, index=False)
    print(df)


if __name__ == "__main__":
    main()
