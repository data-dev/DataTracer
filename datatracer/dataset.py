import os
import json

import pandas as pd


class Field:

    def __init__(self, metadata, data, table):
        self.table = table
        self.metadata = metadata
        self.data = data

        self.name = metadata['name']


class Table:

    def __init__(self, metadata, data, dataset):
        self.dataset = dataset
        self.metadata = metadata
        self.data = data

        self.name = metadata['name']

        self.fields = dict()
        self.field_names = list()
        for field in metadata['fields']:
            field_name = field['name']
            self.field_names.append(field_name)
            self.fields[field_name] = Field(field, data[field['name']], self)

    def __repr__(self):
        return 'Table: {}'.format(self.metadata['name'])


class Dataset:

    def __init__(self, metadata, tables=None, root_path=None):
        if isinstance(metadata, str):
            if not root_path:
                root_path = os.path.dirname(os.path.abspath(metadata))

            with open(metadata) as metadata_file:
                metadata = json.load(metadata_file)

        elif not root_path:
            root_path = ps.getcwd()

        self.metadata = metadata
        self.root_path = root_path

        if tables is None:
            self.tables = dict()
            self.table_names = list()
            for table_meta in self.metadata['tables']:
                table_name = table_meta['name']
                self.table_names.append(table_name)

                table_file = table_meta.get('path') or table_name + '.csv'
                table_data = pd.read_csv(os.path.join(self.root_path, table_file))
                self.tables[table_name] = Table(table_meta, table_data, self)
        else:
            self.tables = tables
            self.table_names = list(tables.keys())
