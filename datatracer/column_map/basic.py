import logging

from sklearn.ensemble import RandomForestRegressor

from datatracer.column_map.base import ColumnMapSolver
from datatracer.column_map.transformer import Transformer

LOGGER = logging.getLogger(__name__)


class BasicColumnMapSolver(ColumnMapSolver):
    """Basic Solver for the data lineage problem of column dependency."""

    def __init__(self, *args, **kwargs):
        self._model_args = args
        self._model_kwargs = kwargs

    def _get_importances(self, X, y):
        model = RandomForestRegressor(*self._model_args, **self._model_kwargs)
        model.fit(X, y)

        return model.feature_importances_

    def solve(self, tables, foreign_keys, target_table=None, target_field=None):
        """Find the fields which contributed to the target_field the most.

        The output is a dictionary containing the fields that contributed the
        most to the given target field as keys, specified as a tuple containing
        both table name and field name, and the score obtained as values.

        Args:
            tables (dict):
                Dict containing table names as input and ``pandas.DataFrames``
                as values.
            foreign_keys (list):
                List of foreign key specifications.
            target_table (str):
                Name of the table that contains the target field. If not given,
                apply on all the tables.
            target_field (str):
                Name of the target field. If not given, apply on all the fields.

        Returns:
            dict:
                Dictionary of field specification tuples and scores.
        """
        transformer = Transformer(tables, foreign_keys)

        if target_table:
            tables = {target_table: tables[target_table]}
        elif target_field:
            raise TypeError('target_field can only be specified if target_column also is')

        column_maps = {}
        for table_name, table in tables.items():
            column_map = {}
            column_maps[table_name] = column_map

            if target_field is not None:
                fields = [target_field]
            else:
                fields = table.keys()

            for field_name in fields:
                if field_name not in table.columns:
                    raise KeyError('Field {} not in table {}'.format(field_name, table_name))

                if field_name in table.select_dtypes('number'):
                    X, y = transformer.forward(table_name, field_name)

                    importances = self._get_importances(X, y)
                    column_map[field_name] = transformer.backward(importances)
                elif target_field:
                    LOGGER.warn(
                        'Mapping a non-numerical field is not supported by this solver',
                    )
                else:
                    LOGGER.info('Skipping unsupported non-numerical field %s.%s',
                                table_name, field_name)

        return column_maps
