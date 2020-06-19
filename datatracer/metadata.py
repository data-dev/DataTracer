"""Metadata releated primitives module.

Primitives implemented so far are:
    * update_metadata_primary_keys: Set primary keys into the given Metadata
    * update_metadata_foreign_keys: Set foreign keys into the given Metadata
    * update_metadata_column_map: Set lineage constraints into the given Metadata
"""

import copy

from metad import MetaData


def validate(metadata):
    metad = MetaData()
    metad.data = metadata
    metad.validate()


def find_object(objects, filters):
    for obj in objects:
        for key, value in filters.items():
            if obj[key] != value:
                break

        else:
            # Reachable only if loop was not broken
            return obj


def _add_primary_keys(metadata_dict, primary_keys):
    primary_keys = copy.deepcopy(primary_keys)
    tables = {
        table['id']: table
        for table in metadata_dict['tables']
    }
    for table_id, primary_key in primary_keys.items():
        table = tables[table_id]
        table['primary_key'] = primary_key


def _add_foreign_keys(metadata_dict, foreign_keys):
    metadata_foreign_keys = metadata_dict.setdefault('foreign_keys', [])
    for foreign_key in foreign_keys:
        if foreign_key not in metadata_foreign_keys:
            metadata_foreign_keys.append(foreign_key)


def _add_column_map(metadata_dict, column_map, target_table, target_field):
    related_fields = [
        {
            'table': table_id,
            'field': field
        }
        for (table_id, field), score in reversed(sorted(column_map.items(), key=lambda x: x[1]))
    ]

    constraint = {
        'constraint_type': 'lineage',
        'fields_under_consideration': [
            {
                'table': target_table,
                'field': target_field
            }
        ]
    }

    constraints = metadata_dict.setdefault('constraints', [])
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


def _load_metadata_dict(metadata):
    """Return the given metadata as a dict."""
    if isinstance(metadata, MetaData):
        return metadata.data

    if isinstance(metadata, str):
        metadata = MetaData.from_json(metadata)
        metadata.validate()
        return metadata.data

    if isinstance(metadata, dict):
        validate(metadata)
        return metadata

    raise TypeError('Metadata can only be MetaData, dict or str')


def _update_metadata(function, metadata, output_path=None, **kwargs):
    metadata_dict = _load_metadata_dict(metadata)
    function(metadata_dict, **kwargs)

    validate(metadata_dict)

    if not isinstance(metadata, MetaData):
        metadata = MetaData()
        metadata.data = metadata_dict

    if output_path is not None:
        metadata.to_json(output_path)

    return metadata


def update_metadata_primary_keys(metadata, primary_keys, output_path=None):
    """Add primary keys to the given metadata.

    Args:
        metadata (MetaData, dict or str):
            Metadata that will be updated. It can be given as a MetaData
            instance, or as a metadata dict or as a path to a metadata
            JSON file.
        primary_keys (dict):
            Dict of primary key specifications.
        output_path (str):
            Optional. If given, store the updated metadata in the given
            path.

    Returns:
        MetaData:
            The updated metadata
    """
    return _update_metadata(
        _add_primary_keys,
        metadata,
        output_path,
        primary_keys=primary_keys
    )


def update_metadata_foreign_keys(metadata, foreign_keys, output_path=None):
    """Add foreign keys to the given metadata.

    Args:
        metadata (MetaData, dict or str):
            Metadata that will be updated. It can be given as a MetaData
            instance, or as a metadata dict or as a path to a metadata
            JSON file.
        foreign_keys (list):
            List of foreign key specifications.
        output_path (str):
            Optional. If given, store the updated metadata in the given
            path.

    Returns:
        MetaData:
            The updated metadata
    """
    return _update_metadata(
        _add_foreign_keys,
        metadata,
        output_path,
        foreign_keys=foreign_keys
    )


def update_metadata_column_map(metadata, column_map, target_table,
                               target_field, output_path=None):
    """Add a column map to the given metadata.

    Args:
        metadata (MetaData, dict or str):
            Metadata that will be updated. It can be given as a MetaData
            instance, or as a metadata dict or as a path to a metadata
            JSON file.
        column_map (dict):
            Column map dict.
        target_table (str):
            If of the target table.
        target_field (str):
            Name of the target field from the target table.
        output_path (str):
            Optional. If given, store the updated metadata in the given
            path.

    Returns:
        MetaData:
            The updated metadata
    """
    return _update_metadata(
        _add_column_map,
        metadata,
        output_path,
        column_map=column_map,
        target_table=target_table,
        target_field=target_field
    )
