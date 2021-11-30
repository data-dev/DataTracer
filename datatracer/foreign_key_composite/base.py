"""Composite Foreign Key Solving base class."""


class CompositeForeignKeySolver():

    def fit(self, dict_of_databases):
        """Fit this solver.

        Args:
            dict_of_databases (dict):
                Map from database names to tuples containing ``MetaData``
                instances and table dictionaries, which contain table names
                as input and ``pandas.DataFrames`` as values.
        """

    def solve(self, tables, primary_keys=None):
        """Solve the foreign key detection problem.

        The output is a list of foreign key specifications, in order from the most likely
        to the least likely.

        Args:
            tables (dict):
                Dict containing table names as input and ``pandas.DataFrames``
                as values.
            primary_keys (dict):
                (Optional). Dictionary of table primary keys, as returned by the Primary
                Key Solvers. This parameter is optional and not all the subclasses need it.

        Returns:
            dict:
                List of foreign key specifications, sorted by likelyhood.
        """
        raise NotImplementedError()
