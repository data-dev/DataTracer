import os
import shutil

import pandas as pd
from metad import MetaData


def load_dataset(dataset_path):
    """Load a dataset as a MetaData and a dict of tables.

    Args:
        dataset_path (str):
            Path to the root of the dataset.

    Returns:
        tuple:
            Tuple containing:
                * A ``MetaData`` instance.
                * A ``dict`` containing the tables loaded as ``pandas.DataFrames``.
    """
    metadata = MetaData.from_json(os.path.join(dataset_path, 'metadata.json'))
    tables = {}
    for table_name in metadata.get_table_names():
        tables[table_name] = pd.read_csv(os.path.join(dataset_path, table_name + '.csv'))

    return metadata, tables


def load_datasets(datasets_path):
    """Load a collection of datasets.

    The datasets are loaded as tuples containing a MetaData instance
    and a dict with the tables of the dataset loaded as DataFrames.

    Args:
        datasets_path (str):
            Path to the folder where the dataset can be found.

    Returns:
        dict:
            Dict of (metadata, tables) tubles, one for each dataset.
    """
    return {
        dataset: load_dataset(os.path.join(datasets_path, dataset))
        for dataset in os.listdir(datasets_path)
    }


def get_demo_data(path='datatracer_demo', force=False):
    """Get a folder with demo data inside it.

    By default, the demo folder will be called ``datatracer_demo`` and will
    be placed inside the current working directory. Optionally, a different
    path can be passed.

    If the destination path exists and force is True it will be removed.
    Otherwise, an error will be raised.

    Args:
        path (str):
            Destination path where the folder will be created, including the
            folder name. Defaults to ``datatracer_demo``.
        force (bool):
            Whether to delete the destination folder if it already exists.
            Defaults to ``False``.

    Raises:
        FileExistsError:
            If the destination folder already exists and force is False.
    """
    if os.path.exists(path):
        if force:
            shutil.rmtree(path)
        else:
            msg = 'Path "{}" already exists. Please remove it or use `force=True`'.format(path)
            raise FileExistsError(msg)
    else:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

    if os.path.isabs(path):
        print('Generating a demo folder at `{}`'.format(path))
    else:
        print('Generating a demo folder at `./{}`'.format(path))
        path = os.path.abspath(path)

    demo_path = os.path.join(os.path.dirname(__file__), 'datasets')
    shutil.copytree(demo_path, path)
