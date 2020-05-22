import numpy as np
from sklearn.ensemble import RandomForestClassifier

from datatracer.primary_key.base import PrimaryKeySolver


class BasicPrimaryKeySolver(PrimaryKeySolver):

    def __init__(self, *args, **kwargs):
        self._model_args = args
        self._model_kwargs = kwargs

    def fit(self, list_of_databases):
        X, y = [], []
        for metadata, tables in list_of_databases:
            for table in metadata.get_tables():
                if "primary_key" not in table:
                    continue
                if not isinstance(table["primary_key"], str):
                    continue

                primary_key = table["primary_key"]
                for column in tables[table["name"]].columns:
                    X.append(self._feature_vector(tables[table["name"]], column))
                    y.append(1.0 if primary_key == column else 0.0)

        X, y = np.array(X), np.array(y)

        self.model = RandomForestClassifier(*self._model_args, **self._model_kwargs)
        self.model.fit(X, y)

    def solve(self, tables):
        primary_keys = {}
        for table in tables:
            primary_keys[table] = self._find_primary_key(tables[table])

        return primary_keys

    def _find_primary_key(self, table):
        best_column, best_score = None, float("-inf")
        for column in table.columns:
            score = self.model.predict([self._feature_vector(table, column)])
            if score > best_score:
                best_column = column
                best_score = score

        return best_column

    def _feature_vector(self, table, column_name):
        column = table[column_name]
        return [
            list(table.columns).index(column_name),
            list(table.columns).index(column_name) / len(table.columns),
            1.0 if len(set(column)) == len(column) else 0.0,
            len(set(column)) / len(column),
            1.0 if "key" in column.name else 0.0,
            1.0 if "id" in column.name else 0.0,
            1.0 if "_key" in column.name else 0.0,
            1.0 if "_id" in column.name else 0.0,
            1.0 if column.dtype == "int64" else 0.0,
            1.0 if column.dtype == "object" else 0.0,
        ]
