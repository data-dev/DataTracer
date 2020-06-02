"""Column Mapping base class."""


class ColumnMapSolver:
    """Base Solver for the data lineage problem of column dependency."""

    def fit(self, list_of_databases):
        """Fit this solver.

        Args:
            list_of_databases (list):
                List of tuples containing ``MetaData`` instnces and table dictinaries,
                which contain table names as input and ``pandas.DataFrames`` as values.
        """
        pass

    def solve(self, tables, foreign_keys, target_table=None, target_field=None):
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
                Name of the table that contains the target field. If not given,
                apply on all the tables.
            target_field (str):
                Name of the target field. If not given, apply on all the fields.
                Accepted only if target_table is also passed.

        Returns:
            dict:
                Dictionary of field specification tuples and scores.

        Raises:
            TypeError:
                If target_field is passed but target_table is not.

        """
        raise NotImplementedError()
