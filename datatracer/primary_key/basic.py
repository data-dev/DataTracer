from .base import PrimaryKeySolver


class BasicPrimaryKeySolver(PrimaryKeySolver):

    def solve(self, tables):
        primary_keys = {}
        for table in tables:
            primary_keys[table] = self._find_primary_key(tables[table])
        return primary_keys

    def _find_primary_key(self, table):
        best_column, best_score = None, float("-inf")
        for column in table.columns:
            score = 0.0
            if len(set(table[column])) != len(table):
                score = float("-inf")
            if "key" in column or "id" in column:
                score += 1.0
            if table[column].dtype in ["int64", "object"]:
                score += 1.0
            if score > best_score:
                best_score = score
                best_column = column
        return best_column
