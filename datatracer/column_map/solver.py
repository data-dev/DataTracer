from sklearn.ensemble import RandomForestRegressor


class Solver():
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
        regr = RandomForestRegressor(
            max_depth=self.depth,
            n_estimators=self.n_estimators
        )
        regr.fit(X, y)
        return regr.feature_importances_
