"""Column Mapping base class."""


class HowLineageSolver:
    """Base Solver for the data lineage problem of how lineage."""

    def fit(self, dict_of_databases):
        """Fit this solver.

        Args:
            dict_of_databases (dict):
                Map from database names to tuples containing ``MetaData``
                instances and table dictionaries, which contain table names
                as input and ``pandas.DataFrames`` as values.
        """

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
        raise NotImplementedError()
