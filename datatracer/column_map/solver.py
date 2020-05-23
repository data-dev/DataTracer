from sklearn.ensemble import RandomForestRegressor

from datatracer.column_map.transformer import Transformer


class ColumnMapSolver:
    """
    Solves the data lineage problem for the column dependency case using random forest regressor.
    """

    def __init__(self, *args, **kwargs):
        self._model_args = args
        self._model_kwargs = kwargs

    def _get_importances(self, X, y):
        """
        Predicts which columns of the input table generated the output column, using
        random forest regressor.
        Returns these columns as a list of column names, chosen from y.
        """
        model = RandomForestRegressor(*self._model_args, **self._model_kwargs)
        model.fit(X, y)

        return model.feature_importances_

    def solve(self, tables, foreign_keys, target_table, target_field):
        """Find the fields which contributed to the target_field."""
        transformer = Transformer(tables, foreign_keys)
        X, y = transformer.forward(target_table, target_field)

        importances = self._get_importances(X, y)
        return transformer.backward(importances)
