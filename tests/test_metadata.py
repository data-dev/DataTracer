import os

import pytest
from metad import MetaData

from datatracer import metadata


@pytest.fixture
def metad():
    metadata = MetaData()
    metadata.data = {
        'tables': [
            {
                'id': '1234',
                'name': 'a_table',
                'fields': [
                    {
                        'name': 'a_field',
                        'type': 'number',
                        'subtype': 'float'
                    },
                    {
                        'name': 'some_field',
                        'type': 'number',
                        'subtype': 'float'
                    }
                ]
            },
            {
                'id': '4567',
                'name': 'another_table',
                'fields': [
                    {
                        'name': 'another_field',
                        'type': 'number',
                        'subtype': 'float'
                    },
                    {
                        'name': 'some_other_field',
                        'type': 'number',
                        'subtype': 'float'
                    }
                ]
            },
        ],
    }
    return metadata


@pytest.fixture
def primary_keys():
    return {
        '1234': 'a_field',
        '4567': 'another_field'
    }


@pytest.fixture
def foreign_keys():
    return [
        {
            'table': '4567',
            'field': 'another_field',
            'ref_table': '1234',
            'ref_field': 'a_field'
        }
    ]


@pytest.fixture
def column_map():
    return {
        ('4567', 'some_other_field'): 0.8,
        ('1234', 'some_field'): 0.9,
    }


@pytest.fixture
def constraints():
    return [
        {
            'constraint_type': 'lineage',
            'fields_under_consideration': [
                {
                    'table': '1234',
                    'field': 'a_field'
                }
            ],
            'related_fields': [
                {
                    'table': '1234',
                    'field': 'some_field'
                },
                {
                    'table': '4567',
                    'field': 'some_other_field'
                }
            ]
        }
    ]


def test_primary_keys(metad, primary_keys):
    updated = metadata.update_metadata_primary_keys(metad, primary_keys)

    assert updated.data['tables'][0]['primary_key'] == 'a_field'
    assert updated.data['tables'][1]['primary_key'] == 'another_field'


def test_primary_keys_dict(metad, primary_keys):
    updated = metadata.update_metadata_primary_keys(metad.data, primary_keys)

    assert updated.data['tables'][0]['primary_key'] == 'a_field'
    assert updated.data['tables'][1]['primary_key'] == 'another_field'


def test_primary_keys_str(metad, primary_keys, tmpdir):
    metad_path = os.path.join(tmpdir, 'metadata.json')
    metad.to_json(metad_path)
    updated = metadata.update_metadata_primary_keys(metad_path, primary_keys)

    assert updated.data['tables'][0]['primary_key'] == 'a_field'
    assert updated.data['tables'][1]['primary_key'] == 'another_field'


def test_primary_output_path(metad, primary_keys, tmpdir):
    output_path = os.path.join(tmpdir, 'metadata.json')
    metadata.update_metadata_primary_keys(metad, primary_keys, output_path=output_path)

    updated = MetaData.from_json(output_path)
    assert updated.data['tables'][0]['primary_key'] == 'a_field'
    assert updated.data['tables'][1]['primary_key'] == 'another_field'


def test_foreign_keys(metad, foreign_keys):
    updated = metadata.update_metadata_foreign_keys(metad, foreign_keys)

    assert updated.data['foreign_keys'] == foreign_keys


def test_foreign_keys_dict(metad, foreign_keys):
    updated = metadata.update_metadata_foreign_keys(metad.data, foreign_keys)

    assert updated.data['foreign_keys'] == foreign_keys


def test_foreign_keys_str(metad, foreign_keys, tmpdir):
    metad_path = os.path.join(tmpdir, 'metadata.json')
    metad.to_json(metad_path)
    updated = metadata.update_metadata_foreign_keys(metad_path, foreign_keys)

    assert updated.data['foreign_keys'] == foreign_keys


def test_foreign_keys_output_path(metad, foreign_keys, tmpdir):
    output_path = os.path.join(tmpdir, 'updated.json')
    metadata.update_metadata_foreign_keys(metad, foreign_keys, output_path=output_path)

    updated = MetaData.from_json(output_path)
    assert updated.data['foreign_keys'] == foreign_keys


def test_column_map(metad, column_map, constraints):
    updated = metadata.update_metadata_column_map(
        metad, column_map, target_table='1234', target_field='a_field')

    assert updated.data['constraints'] == constraints


def test_column_map_dict(metad, column_map, constraints):
    updated = metadata.update_metadata_column_map(
        metad.data, column_map, target_table='1234', target_field='a_field')

    assert updated.data['constraints'] == constraints


def test_column_map_str(metad, column_map, constraints, tmpdir):
    metad_path = os.path.join(tmpdir, 'metadata.json')
    metad.to_json(metad_path)
    updated = metadata.update_metadata_column_map(
        metad_path, column_map, target_table='1234', target_field='a_field')

    assert updated.data['constraints'] == constraints


def test_column_map_output_path(metad, column_map, constraints, tmpdir):
    output_path = os.path.join(tmpdir, 'metadata.json')
    metadata.update_metadata_column_map(
        metad, column_map, target_table='1234', target_field='a_field', output_path=output_path)

    updated = MetaData.from_json(output_path)
    assert updated.data['constraints'] == constraints
