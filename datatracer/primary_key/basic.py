import os
import pickle
import numpy as np
from .base import PrimaryKeySolver

from sklearn.ensemble import RandomForestClassifier

PATH_TO_MODEL = os.path.join(os.path.dirname(__file__), "basic.pkl")

class BasicPrimaryKeySolver(PrimaryKeySolver):

    def __init__(self):
        self.model = RandomForestClassifier()
        if os.path.exists(PATH_TO_MODEL):
            with open(PATH_TO_MODEL, "rb") as fp:
                self.model = pickle.load(fp)
    
    def overwrite_default(self, path_to_model=None):
        if not path_to_model:
            path_to_model = PATH_TO_MODEL
        with open(path_to_model, "wb") as fp:
            pickle.dump(self.model, fp)

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
            1.0 if len(set(column)) == len(column) else 0.0,
            1.0 if "key" in column.name else 0.0,
            1.0 if "id" in column.name else 0.0,
            1.0 if "_key" in column.name else 0.0,
            1.0 if "_id" in column.name else 0.0,
            1.0 if column.dtype == "int64" else 0.0,
            1.0 if column.dtype == "object" else 0.0,
        ]
