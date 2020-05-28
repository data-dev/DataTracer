"""Foreign Key Solving base class."""


class ForeignKeySolver():

    def fit(self, list_of_databases):
        """Fit this solver.

        Args:
            list_of_databases (list):
                List of tuples containing ``MetaData`` instnces and table dictinaries,
                which contain table names as input and ``pandas.DataFrames`` as values.
        """
        pass

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
