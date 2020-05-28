from sklearn.ensemble import RandomForestRegressor

from datatracer.column_map.transformer import Transformer


class ColumnMapSolver:
    """Solver for the data lineage problem of column dependency."""

    def __init__(self, *args, **kwargs):
        self._model_args = args
        self._model_kwargs = kwargs

    def _get_importances(self, X, y):
        model = RandomForestRegressor(*self._model_args, **self._model_kwargs)
        model.fit(X, y)

        return model.feature_importances_

    def solve(self, tables, foreign_keys, target_table, target_field):
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
                Name of the table that contains the target field.
            target_field (str):
                Name of the target field.

        Returns:
            dict:
                Dictionary of field specification tuples and scores.
        """
        transformer = Transformer(tables, foreign_keys)
        X, y = transformer.forward(target_table, target_field)

        importances = self._get_importances(X, y)
        return transformer.backward(importances)
