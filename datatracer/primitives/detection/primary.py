import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RandomizedSearchCV

from datatracer.utils import get_tables_list


class BasicPrimaryKeyDetector:

    def __init__(self, *args, **kwargs):
        self.model = RandomForestClassifier(*args, **kwargs)
        # self.model = RandomizedSearchCV(RandomForestClassifier(), {
        #     "n_estimators": [100, 1000],
        #     "criterion": ["gini", "entropy"],
        #     "max_depth": [None, 1, 2, 3]
        # })

    def _extract_features(self, field):
        table = field.table
        field_names = table.field_names
        field_index = field_names.index(field.name)
        num_rows = table.metadata['number_of_rows']
        num_uniques = field.metadata['number_of_uniques']
        data_type = field.metadata['data_type']
        subtype = field.metadata.get('subtype')
        return [
            field_index,
            field_index / len(field_names),
            num_uniques == num_rows,
            num_uniques / num_rows,
            "key" in field.name,
            "id" in field.name,
            "_key" in field.name,
            "_id" in field.name,
            data_type == 'numerical' and subtype == 'integer',
            data_type == 'categorical'
        ]

    def _make_feature_matrix(self, tables, fit=False):
        X = list()
        if fit:
            y = list()

        for table in tables:
            if fit:
                primary = table.metadata['primary_key']

            for name, field in table.fields.items():
                X.append(self._extract_features(field))
                if fit:
                    y.append(name == primary)

        if fit:
            return np.array(X), np.array(y)
        else:
            return np.array(X)

    def fit(self, data):
        tables = get_tables_list(data)
        X, y = self._make_feature_matrix(tables, fit=True)
        self.model.fit(X, y)

    def detect(self, data):
        tables = get_tables_list(data)
        X = self._make_feature_matrix(tables, fit=False)
        y = self.model.predict_proba(X)[:, 1]

        for proba, table in zip(y, tables):
            index = np.argmax(proba)
            table.metadata['primary_key'] = table.field_names[index]
