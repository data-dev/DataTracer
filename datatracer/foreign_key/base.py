
class ForeignKeySolver():

    def fit(self, list_of_databases):
        for metadata, tables in list_of_databases:
            pass  # do stuff

    def solve(self, tables, primary_keys):
        """
        Return a list of foreign keys in order from most likely to least likely.
        """
        raise NotImplementedError()
