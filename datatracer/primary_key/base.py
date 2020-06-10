"""Primary Key Solving base class."""


class PrimaryKeySolver():

    def fit(self, dict_of_databases):
        """Fit this solver.

        Args:
            dict_of_databases (dict):
                Map from database names to tuples containing ``MetaData`` 
                instances and table dictionaries, which contain table names 
                as input and ``pandas.DataFrames`` as values.
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
