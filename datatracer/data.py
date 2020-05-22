import os

import pandas as pd
from metad import MetaData


def load_dataset(dataset_path):
    metadata = MetaData.from_json(os.path.join(dataset_path, 'metadata.json'))
    tables = {}
    for table_name in metadata.get_table_names():
        tables[table_name] = pd.read_csv(os.path.join(dataset_path, table_name + '.csv'))

    return metadata, tables
