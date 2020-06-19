"""DataTracer functional and REST api.

This module implements a set of simple python functions
to expose the different types of solvers from DataTracer
in a simplified functional interface.

This functional interface is also exposed through a REST
API based on [hug](https://hug.rest).
"""

import copy
import io
import json
import uuid

import hug
import pandas as pd
from hug import types

from datatracer import DataTracer
from datatracer.metadata import find_object, validate

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
        table_details = copy.deepcopy(table)
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


def _update_primary_keys(metadata_dict, primary_keys):
    """Add primary keys to the given metadata.

    Args:
        metadata_dict (dict):
            Dictionary representing a dataset metadata. The dictionary
            corresponds to the ``data`` attribute of a ``MetaData`` object.
        primary_keys (list):
            List containing a set of attributes that uniquely identify a
            table from the given metadata and an additional ``primary_key``
            entry indicating the name of the field that acts as the primary
            key of the table.
    """
    primary_keys = copy.deepcopy(primary_keys)
    tables = metadata_dict['tables']
    for primary_key_spec in primary_keys:
        primary_key_column = primary_key_spec.pop('primary_key')
        table = find_object(tables, primary_key_spec)
        table['primary_key'] = primary_key_column


def _update_foreign_keys(metadata, foreign_keys):
    """Add foreign keys to the given metadata.

    Args:
        metadata_dict (dict):
            Dictionary representing a dataset metadata. The dictionary
            corresponds to the ``data`` attribute of a ``MetaData`` object.
        foreign_keys (list):
            List containing foreign key specifications.
    """
    foreign_keys = copy.deepcopy(foreign_keys)
    tables = metadata['tables']
    metadata_foreign_keys = metadata['foreign_keys']
    for foreign_key in foreign_keys:
        table_filters = foreign_key['table']
        table = find_object(tables, table_filters)
        foreign_key['table'] = table['name']   # Change to `id` on MetaData v0.0.2

        ref_table_filters = foreign_key['ref_table']
        ref_table = find_object(tables, ref_table_filters)
        foreign_key['ref_table'] = ref_table['name']   # Change to `id` on MetaData v0.0.2

        if foreign_key not in metadata_foreign_keys:
            metadata_foreign_keys.append(foreign_key)


def _update_lineage_constraint(metadata, target_table, target_field, column_mappings):
    """Add or update a lineage constraint of the given metadata.

    Args:
        metadata_dict (dict):
            Dictionary representing a dataset metadata. The dictionary
            corresponds to the ``data`` attribute of a ``MetaData`` object.
        target_table (dict):
            Dict containing a set of attributes that uniquely identify a
            table from the given metadata.
        target_field (str):
            Name of a filed in the target table.
        column_mappings (list):
            List containing column mapping specifications.
    """
    tables = metadata['tables']
    target_table = find_object(tables, target_table)['name']   # Change to `id` on MetaData v0.0.2

    related_fields = []
    for column_mapping in column_mappings:
        related_table = find_object(tables, column_mapping['table'])
        related_fields.append({
            'table': related_table['name'],   # Change to `id` on MetaData v0.0.2
            'field': column_mapping['field']
        })

    constraint = {
        'constraint_type': 'lineage',
        'fields_under_consideration': [
            {
                'table': target_table,
                'field': target_field
            }
        ]
    }

    constraints = metadata['constraints']
    metadata_constraint = find_object(constraints, constraint)
    if not metadata_constraint:
        # Constraint does not exist yet, so add it
        constraints.append(constraint)
        constraint['related_fields'] = related_fields
    else:
        # Constraing exists, so just add any missing fields.
        metadata_constraint['related_fields'] = list(set(
            metadata_constraint['related_fields'] + related_fields
        ))


@hug.post('/update_metadata')
def update_metadata(metadata: types.multi(types.json, types.text), update: types.json):
    if isinstance(metadata, dict):
        metadata = copy.deepcopy(metadata)
    else:
        with open(metadata, 'r') as metadata_file:
            metadata = json.load(metadata_file)

    validate(metadata)

    if 'primary_keys' in update:
        _update_primary_keys(metadata, update['primary_keys'])

    if 'foreign_keys' in update:
        _update_foreign_keys(metadata, update['foreign_keys'])

    if 'column_mappings' in update:
        _update_lineage_constraint(
            metadata, update['target_table'], update['target_field'], update['column_mappings'])

    validate(metadata)

    return metadata
