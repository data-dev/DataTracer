from .base import ForeignKeySolver


class BasicForeignKeySolver(ForeignKeySolver):

    def solve(self, tables, primary_keys=None):
        foreign_keys = []
        for t1 in tables:
            for t2 in tables:
                if t1 == t2:
                    continue
                for c1 in tables[t1].columns:
                    for c2 in tables[t2].columns:
                        if tables[t1][c1].dtype != tables[t2][c2].dtype:
                            continue
                        score = self._score(tables[t1][c1], tables[t2][c2])
                        foreign_keys.append((score, t1, c1, t2, c2))
        best_foreign_keys = []
        for score, t1, c1, t2, c2 in sorted(foreign_keys):
            if score > 0.75:
                best_foreign_keys.append({
                    "table": t1,
                    "field": c1,
                    "ref_table": t2,
                    "ref_field": c2
                })
        return best_foreign_keys

    def _score(self, col_a, col_b):
        set_a, set_b = set(col_a), set(col_b)
        if set_b.issubset(set_a):  # child must be subset of parent
            num = len(set_a.intersection(set_b))
            denom = max(len(set_a), len(set_b))
            return num / denom
        return 0.0
