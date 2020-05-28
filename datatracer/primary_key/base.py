"""Primary Key Solving base class."""


class PrimaryKeySolver():

    def fit(self, list_of_databases):
        """Fit this solver.

        Args:
            list_of_databases (list):
                List of tuples containing ``MetaData`` instnces and table dictinaries,
                which contain table names as input and ``pandas.DataFrames`` as values.
        """
        pass

    def solve(self, tables):
        """Solve the primary key detection problem.

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
        raise NotImplementedError()
