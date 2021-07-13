import logging

from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from itertools import combinations
import numpy as np
import time

from datatracer.column_map.base import ColumnMapSolver
from datatracer.column_map.transformer import Transformer

LOGGER = logging.getLogger(__name__)

def approx_equal(num, target, add_margin, multi_margin):
    if target >= 0:
        return (num <= target * (1 + multi_margin) + add_margin) and (num >= target * (1 - multi_margin) - add_margin)
    else:
        return (num <= target * (1 - multi_margin) + add_margin) and (num >= target * (1 + multi_margin) - add_margin)
    
def approx_equal_arrays(num, target, add_margin, multi_margin):
    for n, t in zip(num, target):
        if not approx_equal(n, t, add_margin, multi_margin):
            return False
    return True

def check_sum(indicies, X, y, add_margin, multi_margin):
    return approx_equal_arrays(X[:, indicies].sum(axis = 1), y, add_margin, multi_margin)

def check_avg(indicies, X, y, add_margin, multi_margin):
    return approx_equal_arrays(X[:, indicies].sum(axis = 1)/len(indicies), y, add_margin, multi_margin)

def check_diff(indicies, X, y, add_margin, multi_margin):
    pred_y = X[:, indicies[0]] - X[:, indicies[1]]
    return approx_equal_arrays(pred_y, y, 0, 0)

def detect_restricted_reg(X, y, add_margin=1e-4, mult_margin=1e-4, max_feature=5, timeout=3600):
    """
    This method runs a restricted regression where the target column is either the sum
    or difference of several columns in the given table, or the average of several columns
    in the given table.
    
    Returns:
        (str, tuple): a string ("sum", "diff", "avg" or "None") representing the operation,
        and a tuple of coeffs.
    """
    start_time = time.time()
    
    dot_prods = (X.T).dot(y)
    length = len(dot_prods)
    y2 = y.dot(y)
    for num_feature in range(1, max_feature + 1):
        for combo in combinations(range(length),num_feature):
            if time.time() - start_time > timeout:
                return "None", None
            
            indicies = list(combo)
            if approx_equal(dot_prods[indicies].sum(), y2, add_margin, mult_margin):
                if check_sum(indicies, X, y, add_margin, mult_margin):
                    weights = [0] * length
                    for ind in indicies:
                        weights[ind] = 1
                    return "sum", weights
            if (num_feature > 1) and approx_equal(dot_prods[indicies].sum()/num_feature, y2, add_margin, mult_margin):
                if check_avg(indicies, X, y, add_margin, mult_margin):
                    weights = [0] * length
                    for ind in indicies:
                        weights[ind] = 1.0/num_feature
                    return "avg", weights
            if num_feature == 2:
                if approx_equal(dot_prods[indicies[0]] - dot_prods[indicies[1]], y2, add_margin, mult_margin):
                    if check_diff(indicies, X, y, add_margin, mult_margin):
                        weights = [0] * length
                        weights[indicies[0]] = 1
                        weights[indicies[1]] = -1
                        return "diff", weights
                if approx_equal(dot_prods[indicies[1]] - dot_prods[indicies[0]], y2, add_margin, mult_margin):
                    if check_diff(indicies[::-1], X, y, add_margin, mult_margin):
                        weights = [0] * length
                        weights[indicies[0]] = -1
                        weights[indicies[1]] = 1
                        return "diff", weights
    return "None", None


class BasicColumnMapSolver(ColumnMapSolver):
    """Basic Solver for the data lineage problem of column dependency."""

    def __init__(self, threshold=0.1, *args, **kwargs):
        self._model_args = args
        self._model_kwargs = kwargs
        self._threshold = threshold
        self._linear_weight_threshold = 1e-4
        self._linear_score_threshold = 0.95

    def _get_importances(self, X, y):
        model = RandomForestRegressor(*self._model_args, **self._model_kwargs)
        model.fit(X, y)

        return model.feature_importances_

    def _convert_linear_importances(self, weights):
        new_weights = (weights > self._linear_weight_threshold) / \
            sum(weights > self._linear_weight_threshold)

        return new_weights

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
        if len(X.shape) != 2:  # invalid X shape
            return {}
        elif X.shape[0] == 0 or X.shape[1] == 0:  # empty dimension
            return {}

        try:
            restricted_linear_type, weights = detect_restricted_reg(X, y)
            if restricted_linear_type != "None":
                importances = self._convert_linear_importances(np.array(weights))
            else:
                importances = self._get_importances(X, y)
        except BaseException:
            importances = self._get_importances(X, y)

        ret_dict = transformer.backward(importances)
        flag = True
        while flag:
            flag = False
            new_rets = ret_dict.copy()
            total_score = sum(ret_dict.values())
            for field, score in ret_dict.items():
                if score < total_score * self._threshold / len(ret_dict):
                    del new_rets[field]
                    flag = True
            ret_dict = new_rets
        return ret_dict
