"""DataTracer Benchmark CLI.

Usage:
  datatracer primary <datasets>... [--out=<out>]
  datatracer foreign <datasets>... [--out=<out>]
  datatracer constraint <datasets>... [--out=<out>]

Options:
  -h --help     Show this screen.
  --out=<out>   Path to output file.
"""
import os
from glob import glob

import pandas as pd
from docopt import docopt
from metad import MetaData
from tqdm import tqdm

from datatracer.column_map import ColumnMapSolver
from datatracer.foreign_key import ForeignKeySolver
from datatracer.primary_key import PrimaryKeySolver


def load_dataset(path_to_dataset):
    metadata = MetaData.from_json(path_to_dataset + "/metadata.json")
    tables = {}
    for path_to_csv in glob(path_to_dataset + "/*.csv"):
        table_name = os.path.basename(path_to_csv)
        table_name = table_name.replace(".csv", "")
        tables[table_name] = pd.read_csv(path_to_csv, low_memory=False)

    return metadata, tables


def primary(args):
    results = []

    print("Evaluating primary key detection...")
    for path_to_dataset in args["<datasets>"]:
        print("Loading %s..." % path_to_dataset)
        metadata, tables = load_dataset(path_to_dataset)

        list_of_databases = []
        for path_to_train in tqdm(args["<datasets>"], "loading training set"):
            if path_to_train != path_to_dataset:
                list_of_databases.append(load_dataset(path_to_train))

        for solver in PrimaryKeySolver.__subclasses__():
            solver = solver()
            solver.fit(list_of_databases)

            print("  Testing %s..." % solver.__class__.__name__)
            primary_keys_truth = {}
            for table in metadata.get_tables():
                # skip composite primary keys
                if "primary_key" in table and isinstance(table["primary_key"], str):
                    primary_keys_truth[table["name"]] = table["primary_key"]

            correct, total = 0, 0
            primary_keys_predicted = solver.solve(tables)
            for key, value in primary_keys_truth.items():
                if value:
                    total += 1
                    if value == primary_keys_predicted[key]:
                        correct += 1

            results.append({
                "dataset": path_to_dataset,
                "solver": solver.__class__.__name__,
                "accuracy": correct / total
            })

    df = pd.DataFrame(results).set_index(["dataset", "solver"])
    if args["--out"]:
        df.to_csv(args["--out"])
    else:
        print(df)


def foreign(args):
    results = []

    print("Evaluating foreign key detection...")
    for path_to_dataset in args["<datasets>"]:
        print("Testing %s..." % path_to_dataset)
        list_of_databases = []
        for path_to_train in tqdm(args["<datasets>"], "loading training set"):
            if path_to_train != path_to_dataset:
                list_of_databases.append(load_dataset(path_to_train))

        for solver in ForeignKeySolver.__subclasses__():
            print("  Fitting %s..." % solver.__name__)

            solver = solver()

            solver.fit(list_of_databases)

            metadata, tables = load_dataset(path_to_dataset)

            foreign_keys_true = metadata.get_foreign_keys()
            foreign_keys_predicted = list(solver.solve(tables))

            best_f1, best_precision, best_recall = -1.0, 0.0, 0.0
            fk_true = set()
            for fk in foreign_keys_true:
                fk_true.add((fk["table"], fk["field"], fk["ref_table"], fk["ref_field"]))
            if len(fk_true) == 0:
                continue

            fk_predicted = set()
            for fk in foreign_keys_predicted:
                fk_predicted.add((fk["table"], fk["field"], fk["ref_table"], fk["ref_field"]))
                precision = len(fk_true.intersection(fk_predicted)) / len(fk_predicted)

                recall = len(fk_true.intersection(fk_predicted)) / len(fk_true)

                if precision == 0 or recall == 0:
                    continue
                f1 = 2.0 * precision * recall / (precision + recall)

                if f1 > best_f1:
                    best_f1 = f1
                    best_precision = precision
                    best_recall = recall
            if len(fk_predicted) == 0:
                raise ValueError("No foreign keys predicted!")

            results.append({
                "dataset": path_to_dataset,
                "solver": solver.__class__.__name__,
                "f1": best_f1,
                "precision": best_precision,
                "recall": best_recall
            })
            print(results[-1])

    df = pd.DataFrame(results).set_index(["dataset", "solver"])
    if args["--out"]:
        df.to_csv(args["--out"])
    else:
        print(df)


def constraint(args):
    results = []

    print("Evaluating constraint detection...")
    for path_to_dataset in args["<datasets>"]:
        print("Loading %s..." % path_to_dataset)
        metadata, tables = load_dataset(path_to_dataset)

        # Test One-To-One Constraints
        for constraint in tqdm(metadata.data["constraints"]):
            field = constraint["fields_under_consideration"][0]
            related_fields = constraint["related_fields"]

            solution = ColumnMapSolver().solve(
                tables,
                metadata.get_foreign_keys(),
                target_field=(field["table"], field["field"])
            )
            solution = sorted([
                (score, col)
                for col, score in solution.items()
            ], reverse=True)

            best_f1, best_precision, best_recall = 0.0, 0.0, 0.0
            fk_true = set()
            for related_field in related_fields:
                fk_true.add((related_field["table"], related_field["field"]))
            fk_predicted = set()
            for _, column_name in solution:
                fk_predicted.add(column_name)

                precision = len(fk_true.intersection(fk_predicted)) / len(fk_predicted)

                if len(fk_true) == 0:
                    continue
                recall = len(fk_true.intersection(fk_predicted)) / len(fk_true)

                if precision == 0 or recall == 0:
                    continue
                f1 = 2.0 * precision * recall / (precision + recall)

                if f1 > best_f1:
                    best_f1 = f1
                    best_precision = precision
                    best_recall = recall
            results.append({
                "dataset": path_to_dataset,
                "solver": ColumnMapSolver.__name__,
                "field": "%s.%s" % (field["table"], field["field"]),
                "f1": best_f1,
                "precision": best_precision,
                "recall": best_recall
            })

        df = pd.DataFrame(results).set_index(["dataset", "solver", "field"])
        if args["--out"]:
            df.to_csv(args["--out"])
        else:
            print(df)


def main():
    args = docopt(__doc__)
    if args["primary"]:
        primary(args)
    elif args["foreign"]:
        foreign(args)
    elif args["constraint"]:
        constraint(args)


if __name__ == '__main__':
    main()
