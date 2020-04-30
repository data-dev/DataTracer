"""Metadata.JSON CLI.

Usage:
  datatracer-benchmark <datasets>...

Options:
  -h --help              Show this screen.
"""
import os
from glob import glob

import pandas as pd
from docopt import docopt

from datatracer.column_map import BasicColumnMapSolver
from datatracer.foreign_key import BasicForeignKeySolver
from datatracer.primary_key import BasicPrimaryKeySolver
from metadatajson import MetadataJSON


def load_dataset(path_to_dataset):
    metadata = MetadataJSON.from_json(path_to_dataset + "/metadata.json")
    tables = {}
    for path_to_csv in glob(path_to_dataset + "/*.csv"):
        table_name = os.path.basename(path_to_csv)
        table_name = table_name.replace(".csv", "")
        tables[table_name] = pd.read_csv(path_to_csv)
    return metadata, tables


def primary(args):
    print("Evaluating primary key detection...")
    solver = BasicPrimaryKeySolver()
    for path_to_dataset in args["<datasets>"]:
        metadata, tables = load_dataset(path_to_dataset)

        primary_keys_truth = {}
        for table in metadata.get_tables():
            if isinstance(table["primary_key"], str):
                primary_keys_truth[table["name"]] = table["primary_key"]
            else:
                primary_keys_truth[table["name"]] = None

        correct, total = 0, 0
        primary_keys_predicted = solver.solve(tables)
        for key, value in primary_keys_truth.items():
            if value:
                total += 1
                if value == primary_keys_predicted[key]:
                    correct += 1
        print("%s: Identified %s / %s primary keys." % (path_to_dataset, correct, total))


def foreign(args):
    print("Evaluating foreign key detection...")
    solver = BasicForeignKeySolver()
    for path_to_dataset in args["<datasets>"]:
        metadata, tables = load_dataset(path_to_dataset)
        foreign_keys_true = metadata.get_foreign_keys()
        foreign_keys_predicted = solver.solve(tables)

        foreign_keys_true = [tuple(sorted(list(row.values()))) for row in foreign_keys_true]
        foreign_keys_predicted = [tuple(sorted(list(row.values())))
                                  for row in foreign_keys_predicted]

        correct = len(set(foreign_keys_predicted).intersection(foreign_keys_true))
        total = max(len(foreign_keys_predicted), len(foreign_keys_true))
        print("%s: Identified %s / %s foreign keys." % (path_to_dataset, correct, total))

        solver2 = BasicColumnMapSolver()
        solver2.solve(tables, metadata.get_foreign_keys())


def main():
    args = docopt(__doc__)
    print("-" * 80)
    primary(args)
    print("-" * 80)
    foreign(args)


if __name__ == '__main__':
    main()
