from random import choice

from datatracer.column_map.base import ColumnMapSolver

from sklearn.ensemble import RandomForestRegressor
import pandas as pd
import numpy as np

class RandomForestSolver():
	"""
	Solves the data lineage problem for the column dependency case using random forest regressor.
	"""
	def __init__(self, depth, n_estimators):
		self.depth = depth
		self.n_estimators = n_estimators
	def solve(self, X, y):
		"""
		Predicts which columns of the input table generated the output column, using
		random forest regressor.
		Returns these columns as a list of column names, chosen from y.
		"""
		X_copy, y = X.to_numpy(), y.to_numpy()
		regr = RandomForestRegressor(max_depth=self.depth, n_estimators=self.n_estimators)
		regr.fit(X_copy, y) 
		return self._features_dictionary(regr.feature_importances_, X)
	def _features_dictionary(self, feature_values, X):
		"""
		feature_values: array of real numbers
		X: dataFrame
		return: dictionary pairing each column name of X to the feature value.
		Assumes X columns and feature_values are in the same order.
		"""
		col_names = X.columns.values
		assert len(feature_values) == len(col_names)
		named_features = dict()
		for i in range(len(col_names)):
			assert col_names[i] not in named_features, "There should be no columns with duplicate names."
			named_features[col_names[i]] = feature_values[i]
		return named_features

class BasicColumnMapSolver(ColumnMapSolver):

    def solve(self, tables, foreign_keys, target_field, same_table=True):
        """
        Find the fields which contributed to the target_field. Where target_field
        is a tuple specifying (table, column). If same_table, then only consider
        columns in the same table (i.e. one to one).
        """
        if not same_table:
            raise NotImplementedError("TODO: Implement one to many.")
        table_name, column_name = target_field
        return one_to_one(tables[table_name], column_name)

def one_to_one(df, target_column):
    """
    Return the dictionary mapping each column to a score which indicates how likely it 
    is to have contributed to the target column.
    """
    df = df.select_dtypes("number")
    df = df.fillna(0.0)
    solver = RandomForestSolver(depth=3, n_estimators=1000)
    return solver.solve(df.drop([target_column], axis=1), df[target_column])

if __name__ == "__main__":
    import pandas as pd
    print(one_to_one(pd.DataFrame([
        [1, 1, 1],
        [1, 2, 1],
        [3, 3, 3],
        [1, 4, 1],
        [1, 2, 1],
        [3, 3, 3]
    ], columns=["a", "b", "c"]), target_column="c"))
