from collections import Counter
from itertools import permutations

from sklearn.ensemble import RandomForestClassifier
from tqdm import tqdm

from datatracer.foreign_key_composite.base import CompositeForeignKeySolver


class BasicCompositeForeignKeySolver(CompositeForeignKeySolver):

    def __init__(self, threshold=[i / 20 for i in range(20)], add_details=False, *args, **kwargs):
        self._threshold = threshold
        self._add_details = add_details
        self._model_args = args
        self._model_kwargs = kwargs

    def _diff(self, a, b):
        diff = 0
        a, b = Counter(a), Counter(b)
        for k in set(a.keys()).union(set(b.keys())):
            diff += abs(a[k] - b[k])

        return diff

    def _feature_vector(self, tables, fk):
        table = tables[fk[0]]
        ref_table = tables[fk[2]]
        features = [np.array(self._feature_vector_single_col(ref_table[ref_field], table[field])) for field, ref_field in zip(fk[1], fk[3])]
        return sum(features)/len(features)

    def _feature_vector_single_col(self, parent_col, child_col):
        parent_set, child_set = set(parent_col.unique()), set(child_col.unique())
        len_intersect = len(parent_set.intersection(child_set))
        return [
            len_intersect / (len(child_set) + 1e-5),
            len_intersect / (len(parent_set) + 1e-5),
            len_intersect / (max(len(child_set), len(parent_set)) + 1e-5),
            1.0 if parent_col.name == child_col.name else 0.0,
            self._diff(parent_col.name, child_col.name),
            1.0 if child_set.issubset(parent_set) else 0.0,
            1.0 if parent_set.issubset(child_set) else 0.0,
            len(parent_set) / (len(parent_col) + 1e-5),
            1.0 if "key" in parent_col.name else 0.0,
            1.0 if "id" in parent_col.name else 0.0,
            1.0 if "_key" in parent_col.name else 0.0,
            1.0 if "_id" in parent_col.name else 0.0,
            1.0 if parent_col.dtype == "int64" else 0.0,
            1.0 if parent_col.dtype == "object" else 0.0,
            len(child_set) / (len(child_col) + 1e-5),
            1.0 if "key" in child_col.name else 0.0,
            1.0 if "id" in child_col.name else 0.0,
            1.0 if "_key" in child_col.name else 0.0,
            1.0 if "_id" in child_col.name else 0.0,
            1.0 if child_col.dtype == "int64" else 0.0,
            1.0 if child_col.dtype == "object" else 0.0,
        ]

    def findInd(self, dataset, table, ref_table, ref_keys):
        possible_keys = []
        table_df = dataset[table]
        ref_df = dataset[ref_table]
        
        for ref_key in ref_keys:
            ref_column_unique = set(ref_df[ref_key].unique())
            ref_column_dtype = ref_df[ref_key].dtype.kind
            possible_key = []
            for key in table_df.columns:
                if table_df[key].dtype.kind != ref_column_dtype:
                    continue
                if not set(table_df[key].unique()).issubset(ref_column_unique):
                    continue
                possible_key.append(key)
            if len(possible_key) == 0:
                return []
            possible_keys.append(possible_key)
        ref_values_set = set()
        ref_columns = ref_df[list(ref_keys)]
        for i in range(len(ref_df)):
            ref_values_set.add(tuple(ref_columns.iloc[i]))
        
        fks = []
        candidate_keys = self.cartesianProd(possible_keys)
        for candidate_key in candidate_keys:
            if self.checkInd(table_df, candidate_key, ref_values_set):
                fks.append((table, candidate_key, ref_table, ref_keys))
        
        return fks

    def checkInd(self, table_df, keys, ref_values_set):
        columns = table_df[list(keys)]
        for i in range(len(table_df)):
            if tuple(columns.iloc[i]) not in ref_values_set:
                return False
        return True

    def cartesianProd(self, possible_keys):
        if len(possible_keys) == 1:
            return [(key, ) for key in possible_keys[0]]
        recur_ret = self.cartesianProd(possible_keys[1:])
        ret = []
        for key in possible_keys[0]:
            ret.extend([(key, )+rest for rest in recur_ret])
        return ret

    def tuplizeFk(self, fk):
        if isinstance(fk["field"], list):
            fields = sorted([(field, ref_field) for field, ref_field in zip(fk['field'], fk['ref_field'])], key = lambda x:x[1])
            return (fk['table'], tuple([x[0] for x in fields]), fk['ref_table'], tuple([x[1] for x in fields]))
        else:
            return (fk['table'], (fk['field'],), fk['ref_table'], (fk['ref_field'],))

    def fit(self, dict_of_databases):
        """Fit this solver.

        Args:
            dict_of_databases (dict):
                Map from database names to tuples containing ``MetaData``
                instances and table dictionaries, which contain table names
                as input and ``pandas.DataFrames`` as values.
        """
        X, y = [], []
        iterator = tqdm(dict_of_databases.items())
        for database_name, (metadata, tables) in iterator:
            iterator.set_description("Extracting features from %s" % database_name)
            fks = set([self.tuplizeFk(fk) for fk in metadata.get_foreign_keys()])
            tables_info = {table_info['name']: table_info for table_info in metadata.get_tables()}

            for t1, t2 in permutations(tables.keys(), r=2):
                table = tables_info[t2]
                if "primary_key" not in table:
                    continue
                elif not isinstance(table["primary_key"], str):
                    ref_fields = tuple(sorted(table["primary_key"]))
                else:
                    ref_fields = (table["primary_key"], )
                if len(ref_fields) == 0:
                    continue

                for candidate_fk in self.findInd(tables, t1, t2, ref_fields):
                    X.append(self._feature_vector(tables, candidate_fk))
                    y.append(1.0 if candidate_fk in fks else 0.0)

        self.model = RandomForestClassifier(*self._model_args, **self._model_kwargs)
        self.model.fit(X, y)

        if isinstance(self._threshold, list):
            best_f1 = -float('inf')
            best_threshold = None
            pred_y = self.model.predict(X)
            len_true = sum(y)
            for threshold in self._threshold:
                filtered_y = (pred_y >= threshold).astype(float)
                intersect = sum(filtered_y * y)
                len_pred = sum(filtered_y)
                if intersect * len_true * len_pred == 0:
                    f1 = 0
                else:
                    precision = intersect / len_pred
                    recall = intersect / len_true
                    f1 = 2.0 * precision * recall / (precision + recall)
                if f1 > best_f1:
                    best_f1 = f1
                    best_threshold = threshold
            self._threshold = best_threshold

    def solve(self, tables, primary_keys=None):
        """Solve the foreign key detection problem.

        The output is a list of foreign key specifications, in order from the most likely
        to the least likely.

        Args:
            tables (dict):
                Dict containing table names as input and ``pandas.DataFrames``
                as values.
            primary_keys (dict):
                Dict mapping table names to its primary keys

        Returns:
            dict:
                List of foreign key specifications
        """
        if primary_keys is None: #No primary keys to start with
            return []
        X, foreign_keys = [], []

        for t1, t2 in permutations(tables.keys(), r=2):
            if primary_keys[t2] is None:
                continue
            elif not isinstance(primary_keys[t2], str):
                ref_fields = tuple(sorted(primary_keys[t2]))
            else:
                ref_fields = (primary_keys[t2], )

            for candidate_fk in self.findInd(tables, t1, t2, ref_fields):
                print(candidate_fk)
                X.append(self._feature_vector(tables, candidate_fk))
                foreign_keys.append(candidate_fk)

        if len(foreign_keys) == 0:
            return []

        scores = self.model.predict(X)

        best_foreign_keys = []
        for score, (t1, c1, t2, c2) in sorted(zip(scores, foreign_keys), reverse=True):
            if self._threshold is None or score >= self._threshold:
                foreign_key = {
                    "table": t1,
                    "field": list(c1),
                    "ref_table": t2,
                    "ref_field": list(c2),
                }
                if self._add_details:
                    foreign_key.update({
                        "score": score,
                        "features": self._feature_vector(tables, (t1, c1, t2, c2))
                    })

                best_foreign_keys.append(foreign_key)

        return best_foreign_keys