"""DataTracer functional and REST api.

This module implements a set of simple python functions
to expose the different types of solvers from DataTracer
in a simplified functional interface.

This functional interface is also exposed through a REST
API based on [hug](https://hug.rest).
"""

import io
import uuid
from copy import deepcopy

import hug
import pandas as pd
from hug import types

from datatracer import DataTracer

PRIMARY_KEY_SOLVER = 'datatracer.primary_key.basic'
FOREIGN_KEY_SOLVER = 'datatracer.foreign_key.standard'
COLUMN_MAP_SOLVER = 'datatracer.column_map.basic'
COLUMN_MAP_THRESHOLD = 0.1


def _load_table_data(table):
    data = table.pop('data', None)
    if data is not None:
        if isinstance(data, pd.DataFrame):
            return data

        with io.StringIO(data) as sio:
            return pd.read_csv(sio)

    path = table.get('path')
    if path is None:
        raise ValueError('Either `data` or `path` must be provided')

    return pd.read_csv(path)


def _get_table_id(details, table):
    selected = []
    for table_id, table_details in details.items():
        for key, value in table.items():
            if key not in table_details or value != table_details[key]:
                continue

            selected.append(table_id)

    if len(selected) > 1:
        raise ValueError('More than one table matches the given table identifier')
    elif not selected:
        raise ValueError('No table matches the given table identifier')

    return selected[0]


def load_tables(tables):
    data = {}
    details = {}
    for table in tables:
        table_id = uuid.uuid4()
        table_details = deepcopy(table)
        data[table_id] = _load_table_data(table_details)
        details[table_id] = table_details

    return data, details


@hug.post('/primary_key_detection')
def primary_key_detection(tables: types.multiple):
    """Solve a primary key detection problem."""
    tables, details = load_tables(tables)

    solver = DataTracer.load(PRIMARY_KEY_SOLVER)
    primary_keys = solver.solve(tables)

    response = []
    for table_id, primary_key in primary_keys.items():
        table_details = details[table_id]
        table_details['primary_key'] = primary_key
        response.append(table_details)

    return {
        'primary_keys': response
    }


@hug.post('/foreign_key_detection')
def foreign_key_detection(tables: types.multiple):
    """Solve a foreign key detection problem."""
    tables, details = load_tables(tables)

    solver = DataTracer.load(FOREIGN_KEY_SOLVER)
    foreign_keys = solver.solve(tables)

    for foreign_key in foreign_keys:
        foreign_key['table'] = details[foreign_key['table']]
        foreign_key['ref_table'] = details[foreign_key['ref_table']]

    return {
        'foreign_keys': foreign_keys
    }


@hug.post('/column_mapping')
def column_mapping(tables: types.multiple, target_table: types.json, target_field: types.text):
    """Solve a column mapping problem."""
    tables, details = load_tables(tables)
    target_table_id = _get_table_id(details, target_table)

    solver = DataTracer.load(COLUMN_MAP_SOLVER)
    column_map = solver.solve(tables, target_table=target_table_id, target_field=target_field)

    column_mappings = []
    for (table_id, field), score in column_map.items():
        if score >= COLUMN_MAP_THRESHOLD:
            column_mappings.append({
                'table': details[table_id],
                'field': field,
            })

    return {
        'target_table': target_table,
        'target_field': target_field,
        'column_mappings': column_mappings
    }
