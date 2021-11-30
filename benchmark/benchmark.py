import argparse
import os
from io import BytesIO
from time import ctime
from urllib.parse import urljoin
from urllib.request import urlopen
from zipfile import ZipFile

import boto3
import pandas as pd

from column_map_benchmark import benchmark_column_map
from foreign_key_benchmark import benchmark_foreign_key
from how_lineage_benchmark import benchmark_how_lineage
from primary_key_benchmark import benchmark_primary_key
from primary_key_composite_benchmark import benchmark_primary_key_composite
from foreign_key_composite_benchmark import benchmark_foreign_key_composite
from column_map_composite_benchmark import benchmark_composite_column_map

BUCKET_NAME = 'tracer-data'
DATA_URL = 'http://{}.s3.amazonaws.com/'.format(BUCKET_NAME)


def download(data_dir):
    """Download benchmark datasets from S3.

    This downloads the benchmark datasets from S3 into the target folder in an
    uncompressed format. It skips datasets that have already been downloaded.

    Please make sure an appropriate S3 credential is installed before you call
    this method.

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


def start_with(target, source):
    return len(source) <= len(target) and target[:len(source)] == source


def aggregate(cmd_name):
    cmd_abbrv = {'column': 'ColMap_st',
                 'foreign': 'ForeignKey_st',
                 'primary': 'PrimaryKey_st',
                 'primary_composite': 'CompositePrimaryKey_st',
                 'foreign_composite': 'CompositeForeignKey_st',
                 'column_composite': 'CompositeColMap_st',
                 'how': 'HowLineage_st'
                 }
    if cmd_name not in cmd_abbrv:
        print("Invalid command name!")
        return None  # invalid command name
    cmd_name = cmd_abbrv[cmd_name]
    dfs = []
    for file in os.listdir("Reports"):
        if start_with(file, cmd_name):
            dfs.append(pd.read_csv("Reports/" + file))
    if len(dfs) == 0:
        print("No available test results!")
        return None
    df = pd.concat(dfs, axis=0, ignore_index=True)
    os.system("rm Reports/" + cmd_name + "*")  # Clean up the caches
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
    shared_args.add_argument('--ds_name', type=str,
                             default=None, required=False,
                             help='Name of the dataset to test on. Default is all available datasets.')
    shared_args.add_argument('--problem', type=str,
                             default=None, required=False,
                             help='Name of the tests results to aggregate.')
    shared_args.add_argument('--primitive', type=str,
                             default=None, required=False,
                             help='Name of the primitive to be tested.')

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
        'primary_composite',
        parents=[shared_args],
        help='Composite Primary key benchmark.'
    )
    subparser.set_defaults(command=benchmark_primary_key_composite)

    subparser = command.add_parser(
        'foreign_composite',
        parents=[shared_args],
        help='Composite Foreign key benchmark.'
    )
    subparser.set_defaults(command=benchmark_foreign_key_composite)

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

    subparser = command.add_parser(
        'column_composite',
        parents=[shared_args],
        help='Composite column map benchmark.'
    )
    subparser.set_defaults(command=benchmark_composite_column_map)

    subparser = command.add_parser(
        'how',
        parents=[shared_args],
        help='How lineage benchmark.'
    )
    subparser.set_defaults(command=benchmark_how_lineage)

    subparser = command.add_parser(
        'aggregate',
        parents=[shared_args],
        help='Aggregate separate test results.'
    )
    subparser.set_defaults(command=aggregate)

    return parser


def main():
    parser = _get_parser()
    args = parser.parse_args()
    if args.command == download:
        df = args.command(args.data_dir)
    elif args.command == aggregate:
        df = args.command(args.problem)
    else:
        if args.primitive is None:
            df = args.command(args.data_dir, args.ds_name)
        else:
            df = args.command(args.data_dir, args.ds_name, solver=args.primitive)
    cmd_abbrv = {'column': 'ColMap_',
                 'foreign': 'ForeignKey_',
                 'primary': 'PrimaryKey_',
                 'primary_composite': 'CompositePrimaryKey_',
                 'foreign_composite': 'CompositeForeignKey_',
                 'column_composite': 'CompositeColMap_',
                 'how': 'HowLineage_'
                 }
    cmd_str = {benchmark_column_map: 'ColMap_',
               benchmark_foreign_key: 'ForeignKey_',
               benchmark_primary_key: 'PrimaryKey_',
               benchmark_how_lineage: 'HowLineage_',
               benchmark_primary_key_composite: 'CompositePrimaryKey_',
               benchmark_foreign_key_composite: 'CompositeForeignKey_',
               benchmark_composite_column_map: 'CompositeColMap_',
               aggregate: cmd_abbrv[args.problem] if args.problem in cmd_abbrv else ''
               }
    csv_name = "st_" + args.ds_name + ".csv" if args.ds_name else args.csv
    # st is for recognition in the aggregation step

    if csv_name and (args.command in cmd_str) and (df is not None):
        df.to_csv("Reports/" + cmd_str[args.command] + csv_name, index=False)
    print(df)


if __name__ == "__main__":
    main()
