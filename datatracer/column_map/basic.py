from random import choice

from .base import ColumnMapSolver


def many_to_many(parent_table, child_table):
    """
    Return a map from columns in the child table to a list of columns
    in the parent table.

        {
            cost: [cost_1, cost_2]
        }
    """
    column_map = {}
    for child_column in child_table.columns:
        column_map[child_column] = [choice(parent_table.columns)]
    return column_map


class BasicColumnMapSolver(ColumnMapSolver):

    def solve(self, tables, foreign_keys):
        # For each parent-child table, find relationships
        # return a dictionary mapping table name to column name
        """
        {
            "total": {
                "cost": [
                    ("costs", "cost_1"),
                    ("costs", "cost_2"),
                ]
            }
        }
        """
        column_maps = {}
        for fk in foreign_keys:
            column_maps[fk["table"]] = {}
            column_map = many_to_many(tables[fk["ref_table"]], tables[fk["table"]])
            for child_column, parent_columns in column_map.items():
                column_maps[fk["table"]][child_column] = [
                    (fk["ref_table"], parent_column) for parent_column in parent_columns
                ]
        return column_maps
