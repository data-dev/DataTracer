from itertools import permutations

from tqdm import tqdm

from datatracer.foreign_key.base import ForeignKeySolver


class BasicForeignKeySolver(ForeignKeySolver):

    def __init__(self, threshold=0.9, add_details=False):
        self._threshold = threshold
        self._add_details = add_details

    def solve(self, tables, primary_keys=None):
        foreign_keys = []
        for t1, t2 in tqdm(list(permutations(tables.keys(), r=2))):
            for c1 in tables[t1].columns:
                for c2 in tables[t2].columns:
                    if tables[t1][c1].dtype.kind != tables[t2][c2].dtype.kind:
                        continue

                    score = self._score(tables[t1][c1], tables[t2][c2])
                    foreign_keys.append((score, t1, c1, t2, c2))

        best_foreign_keys = []
        for score, t1, c1, t2, c2 in sorted(foreign_keys, reverse=True):
            if self._threshold is None or score >= self._threshold:
                foreign_key = {
                    "table": t1,
                    "field": c1,
                    "ref_table": t2,
                    "ref_field": c2,
                }
                if self._add_details:
                    foreign_key['score'] = score

                best_foreign_keys.append(foreign_key)

        return best_foreign_keys

    def _score(self, col_a, col_b):
        set_a, set_b = set(col_a), set(col_b)
        if set_b.issubset(set_a):  # child must be subset of parent
            num = len(set_a.intersection(set_b))
            denom = max(len(set_a), len(set_b))
            return num / (denom + 1e-5)

        return 0.0
