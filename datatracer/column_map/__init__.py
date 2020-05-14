from .solver import Solver
from .transformer import Transformer


class ColumnMapSolver():

    def fit(self, list_of_databases):
        for metadata, tables in list_of_databases:
            pass

    def solve(self, tables, foreign_keys, target_field):
        """
        Find the fields which contributed to the target_field. Where target_field
        is a tuple specifying (table, column).
        """
        table_name, column_name = target_field
        transformer = Transformer(tables, foreign_keys)
        X, y = transformer.forward(table_name, column_name)

        solver = Solver(depth=3, n_estimators=1000)
        importances = solver.solve(X, y)
        return transformer.backward(importances)
