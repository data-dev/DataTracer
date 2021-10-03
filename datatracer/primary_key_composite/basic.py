"""Basic Primary Key Solver module."""

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from tqdm import tqdm

from datatracer.primary_key_composite.base import CompositePrimaryKeySolver


class BasicCompositePrimaryKeySolver(CompositePrimaryKeySolver):

    def __init__(self, *args, **kwargs):
        self._model_args = args
        self._model_kwargs = kwargs

    def checkUcc(self, table, columns):
        """
        Theck if a specific column combination is a UCC.
        """
        values_set = set()
        for i in range(len(table)): #brute-force check of the uniqueness of the row values
            val = tuple(table[columns].iloc[i])
            if val in values_set:
                return False
            values_set.add(val)
        return True

    def checkMinimal(self, ucc, all_ucc):
        """
        Check the minimality of a ucc.
        """
        for i in range(len(ucc)): #try knock-out every possible entry and see if the rest is an UCC
            ucc_list = list(ucc)
            del ucc_list[i]
            if tuple(ucc_list) in all_ucc:
                return False
        return True

    def findUCC(self, table, max_col=5):
        """
        Find all minimal UCCs of a table, with the number of columns in the UCC being at
        most max_col.

        Returns: a set of UCCs, where an UCC is expressed in the form of a sorted tuple
        """
        uccs = set() #collection of all minimal UCCs
        
        #Algo: keep a set of non-unique column combinations. For each, we try adding a non-unique
        #  column to it and see if it becomes a minimal UCC
        non_unique_columns = []
        non_ucc_current = set()
        non_ucc_next = set()
        
        for column_name in table.columns: #add all single-column UCC and find out all non-unique columns
            column = table[column_name]
            if column.nunique() == len(column):
                uccs.add((column_name, ))
            else:
                non_unique_columns.append(column_name)
                non_ucc_current.add((column_name, ))
        
        col_count = 1 #counter to make sure num of cols in USS is at most max_col
        while non_ucc_current:
            col_count += 1
            if col_count > max_col:
                break
            for comb in non_ucc_current:
                for col in non_unique_columns:
                    if col in comb: #The column must not be already in the column combination
                        continue
                    
                    new_comb = tuple(sorted(list(comb) + [col])) #Create the potential ucc
                    if not self.checkMinimal(new_comb, uccs): #If a subset is already UCC, terminate
                        continue
                    if new_comb in uccs: #Make sure this potential ucc isn't visited before
                        continue
                    if new_comb in non_ucc_next: #Make sure this potential ucc isn't visited before
                        continue 

                    if self.checkUcc(table, list(new_comb)):
                        uccs.add(new_comb)
                    else:
                        non_ucc_next.add(new_comb)
            non_ucc_current = non_ucc_next
            non_ucc_next = set()
        return uccs

    def _feature_single_vector(self, table, column_name):
        column = table[column_name]
        return [
            list(table.columns).index(column_name),
            0.0 if len(table.columns) == 0 else list(
                table.columns).index(column_name) / len(table.columns),
            1.0 if "key" in column.name else 0.0,
            1.0 if "id" in column.name else 0.0,
            1.0 if "_key" in column.name else 0.0,
            1.0 if "_id" in column.name else 0.0,
            1.0 if column.dtype == "int64" else 0.0,
            1.0 if column.dtype == "object" else 0.0,
            1/max([len(str(item)) for item in column])
        ]

    def singleColFeatures(self, table):
        """
        Compute the feature vectors of every column and return it in a dict
        """
        single_col_features = {}
        for column in table.columns:
            single_col_features[column] = self._feature_single_vector(table, column)
        return single_col_features

    def _feature_vector(self, table, ucc, single_col_features):
        features = list(sum([np.array(single_col_features[col]) for col in ucc])/len(ucc))
        features.append(1/len(ucc))
        return features

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
            for table in metadata.get_tables():
                if "primary_key" not in table:
                    pk = []
                elif not isinstance(table["primary_key"], str):
                    pk = tuple(sorted(table["primary_key"]))
                else:
                    pk = (table["primary_key"], )

                table_df = tables[table["name"]]
                single_col_features = self.singleColFeatures(table_df)

                uccs = self.findUCC(table_df, max_col=2)
                if len(pk) > 0: #in the case that we do have primary key
                    uccs.add(pk)
                for ucc in uccs:
                    X.append(self._feature_vector(table_df, ucc, single_col_features))
                    y.append(1.0 if ucc == pk else 0.0)

        X, y = np.array(X), np.array(y)

        self.model = RandomForestClassifier(*self._model_args, **self._model_kwargs)
        self.model.fit(X, y)

    def _find_primary_key(self, table):
        single_col_features = self.singleColFeatures(table)
        uccs = self.findUCC(table, max_col=2)
        best_score = -float('inf')
        primary_key = None
        
        for ucc in uccs:
            score = self.model.predict([self._feature_vector(table, ucc, single_col_features)])
            score = float(score)
            if score > best_score:
                best_score = score
                primary_key = list(ucc)
        return primary_key

    def solve(self, tables):
        """Solve the problem.

        The output is a dictionary contiaining table names as keys, and the
        name of the field that is most likely to be the primary key as values.

        Args:
            tables (dict):
                Dict containing table names as input and ``pandas.DataFrames``
                as values.

        Returns:
            dict:
                Dict containing table names as keys and field names as values.
        """
        primary_keys = {}
        for table in tables:
            primary_keys[table] = self._find_primary_key(tables[table])

        return primary_keys
