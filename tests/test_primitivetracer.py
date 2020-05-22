from unittest import TestCase

import numpy as np
import pandas as pd

from datatracer import DataTracer


class TestDataTracer(TestCase):

    def setUp(self):
        self.table1 = pd.DataFrame(
            np.array([
                [1, 2, 3],
                [4, 5, 6],
                [7, 8, 9]
            ]),
            columns=['a', 'b', 'c']
        )
        self.table2 = pd.DataFrame(
            np.array([
                [1, 2, 3],
                [4, 5, 6],
                [7, 8, 9]
            ]),
            columns=['d', 'e', 'f']
        )

    def test_check_tables(self):
        DataTracer.check_tables({})
        DataTracer.check_tables({"table1": self.table1})
        DataTracer.check_tables({"table1": self.table1, "table2": self.table2})

        with self.assertRaises(Exception):
            DataTracer.check_tables({
                "hello": "world"
            })

    def test_check_primary_keys(self):
        DataTracer.check_primary_keys({})
        DataTracer.check_primary_keys({"table1": "col1"})
        DataTracer.check_primary_keys({"table1": "col1", "table2": "col2"})

        with self.assertRaises(Exception):
            DataTracer.check_primary_keys("hello")

    def test_check_primary_keys_with_tables(self):
        tables = {"table1": self.table1, "table2": self.table2}
        DataTracer.check_primary_keys({"table1": "a", "table2": "f"}, tables=tables)

        with self.assertRaises(Exception):
            DataTracer.check_primary_keys({"table1": "d", "table2": "f"}, tables)

        with self.assertRaises(Exception):
            DataTracer.check_primary_keys({"table3": "f"}, tables)

    def test_foreign_keys(self):
        DataTracer.check_foreign_keys({})

    def test_foreign_keys_with_tables(self):
        DataTracer.check_foreign_keys({}, tables={})
