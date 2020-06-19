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


def test_foreign_keys(metad, foreign_keys):
    updated = metadata.update_metadata_foreign_keys(metad, foreign_keys)

    assert updated.data['foreign_keys'] == foreign_keys


def test_column_map(metad, column_map, constraints):
    updated = metadata.update_metadata_column_map(metad, column_map, '1234', 'a_field')

    assert updated.data['constraints'] == constraints
