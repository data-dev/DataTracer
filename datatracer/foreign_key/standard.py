from .base import ForeignKeySolver
from sklearn.ensemble import RandomForestClassifier


class StandardForeignKeySolver(ForeignKeySolver):

    def __init__(self):
        self.model = RandomForestClassifier()

    def fit(self, list_of_databases):
        X, y = [], []
        for metadata, tables in list_of_databases:
            fks = metadata.get_foreign_keys()
            fks = set([(fk["ref_table"], fk["ref_field"], fk["table"], fk["field"]) for fk in fks])

            for t1 in tables:
                for t2 in tables:
                    if t1 == t2:
                        continue
                    for c1 in tables[t1].columns:
                        for c2 in tables[t2].columns:
                            if tables[t1][c1].dtype != tables[t2][c2].dtype:
                                continue
                            X.append(self._feature_vector(tables[t1][c1], tables[t2][c2]))
                            y.append(1.0 if (t1, c1, t2, c2) in fks else 0.0)
        self.model.fit(X, y)

    def solve(self, tables, primary_keys=None):
        X, foreign_keys = [], []
        for t1 in tables:
            for t2 in tables:
                if t1 == t2:
                    continue
                for c1 in tables[t1].columns:
                    for c2 in tables[t2].columns:
                        if tables[t1][c1].dtype != tables[t2][c2].dtype:
                            continue
                        X.append(self._feature_vector(tables[t2][c2], tables[t1][c1]))
                        foreign_keys.append((t1, c1, t2, c2))
        if not foreign_keys:
            return []
        scores = self.model.predict(X)

        best_foreign_keys = []
        for score, (t1, c1, t2, c2) in sorted(zip(scores, foreign_keys), reverse=True):
            best_foreign_keys.append({
                "table": t1,
                "field": c1,
                "ref_table": t2,
                "ref_field": c2
            })
        return best_foreign_keys

    def _feature_vector(self, parent_col, child_col):
        parent_set, child_set = set(parent_col), set(child_col)
        return [
            len(parent_set.intersection(child_set)) / (len(child_set)+1e-5),
            len(parent_set.intersection(child_set)) / (len(parent_set)+1e-5),
            1.0 if child_set.issubset(parent_set) else 0.0,
            1.0 if len(set(parent_col)) == len(parent_col) else 0.0,
            1.0 if "key" in parent_col.name else 0.0,
            1.0 if "id" in parent_col.name else 0.0,
            1.0 if "_key" in parent_col.name else 0.0,
            1.0 if "_id" in parent_col.name else 0.0,
            1.0 if parent_col.dtype == "int64" else 0.0,
            1.0 if parent_col.dtype == "object" else 0.0,
            1.0 if len(set(child_col)) == len(child_col) else 0.0,
            1.0 if "key" in child_col.name else 0.0,
            1.0 if "id" in child_col.name else 0.0,
            1.0 if "_key" in child_col.name else 0.0,
            1.0 if "_id" in child_col.name else 0.0,
            1.0 if child_col.dtype == "int64" else 0.0,
            1.0 if child_col.dtype == "object" else 0.0,
        ]
