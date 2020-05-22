# -*- coding: utf-8 -*-

"""DataTracer core module.

This module defines the DataTracer class.
"""

import pandas as pd


class DataTracer():

    def __init__(self, primary_key, foreign_key, column_map):
        self.primary_key = primary_key
        self.foreign_key = foreign_key
        self.column_map = column_map

    def fit(self, list_of_databases):
        """
        Trains the underlying models on the list of databases where each
        database is a tuple containing the metadata and tables.
        """
        self.primary_key.fit(list_of_databases)
        self.foreign_key.fit(list_of_databases)
        self.column_map.fit(list_of_databases)

    def solve(self, tables):
        """
        TODO: This should return a metadata object.
        """
        self.check_tables(tables)

        primary_keys = self.primary_key.solve(tables)
        self.check_primary_keys(primary_keys, tables=tables)

        foreign_keys = self.foreign_key.solve(tables, primary_keys)
        self.check_foreign_keys(foreign_keys, tables=tables)

        column_maps = self.column_map.solve(tables, foreign_keys)
        return {
            "primary_keys": primary_keys,
            "foreign_keys": foreign_keys,
            "column_maps": column_maps
        }

    @staticmethod
    def check_tables(tables):
        assert isinstance(tables, dict)
        for key, value in tables.items():
            assert isinstance(key, str)
            assert isinstance(value, pd.DataFrame)

    @staticmethod
    def check_primary_keys(primary_keys, tables=None):
        assert isinstance(primary_keys, dict)
        for key, value in primary_keys.items():
            assert isinstance(key, str)
            assert isinstance(value, str)

        if tables:
            assert set(primary_keys.keys()) == set(tables.keys())
            for key, value in primary_keys.items():
                assert key in tables
                assert value in tables[key].columns

    @staticmethod
    def check_foreign_keys(foreign_keys, tables=None):
        assert isinstance(foreign_keys, dict)
        for table, (field, ref_table, ref_field) in foreign_keys.items():
            assert isinstance(table, str)
            assert isinstance(field, str)
            assert isinstance(ref_table, str)
            assert isinstance(ref_field, str)

        if tables:
            assert set(foreign_keys.keys()) == set(tables.keys())
            for table, (field, ref_table, ref_field) in foreign_keys.items():
                assert table in tables
                assert field in tables[table]
                assert ref_table in tables
                assert ref_field in tables[ref_table]
