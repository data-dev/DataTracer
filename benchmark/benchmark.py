from datatracer import load_datasets, DataTracer

def evaluate_primary_key(solver, metadata, tables):
    """Evaluate a primary key solver.

    This computes the accuracy of a primary key solver on the given dataset. It
    skips tables without primary keys as well as tables which have composite 
    primary keys.

    Args:
        solver (DataTracer):
            A DataTracer instance which implements primary key detection.
        metadata (MetaData):
            A MetaData instance which describes the tables.
        tables (dict):
            A dictionary mapping table names to dataframes

    Returns:
        dict:
            A dictionary mapping metric names to values.
    """
    y_true = {}
    for table in metadata.get_tables():
        if "primary_key" not in table:
            continue  # Skip tables without primary keys
        if not isinstance(table["primary_key"], str):
            continue  # Skip tables with composite primary keys
        y_true[table["name"]] = table["primary_key"]
    
    correct, total = 0, 0
    y_pred = solver.solve(tables)
    for table_name, primary_key in y_true.items():
        if y_pred.get(table_name) == primary_key:
            correct += 1
        total += 1
    accuracy = correct / total

    return {
        "accuracy": accuracy
    }

def evaluate_foreign_key(solver, metadata, tables):
    """Evaluate a foreign key solver.

    This computes the precision, recall, and f1 score of a foreign key solver 
    on the given dataset. It skips composite foreign primary keys.

    Args:
        solver (DataTracer):
            A DataTracer instance which implements foreign key detection.
        metadata (MetaData):
            A MetaData instance which describes the tables.
        tables (dict):
            A dictionary mapping table names to dataframes

    Returns:
        dict:
            A dictionary mapping metric names to values.
    """
    y_true = set()
    for fk in metadata.get_foreign_keys():
        if not isinstance(fk["field"], str):
            continue  # Skip composite foreign keys
        y_true.add((fk["table"], fk["field"], fk["ref_table"], fk["ref_field"]))

    y_pred = set()
    best_precision, best_recall, best_f1 = float("-inf"), float("-inf"), float("-inf")
    for fk in solver.solve(tables):
        y_pred.add((fk["table"], fk["field"], fk["ref_table"], fk["ref_field"]))

        precision = len(y_true.intersection(y_pred)) / len(y_pred)
        recall = len(y_true.intersection(y_pred)) / len(y_true)
        f1 = 2.0 * precision * recall / (precision + recall)

        if f1 > best_f1:
            best_precision = precision
            best_recall = recall
            best_f1 = f1

    return {
        "precision": best_precision,
        "recall": best_recall,
        "f1": best_f1
    }

if __name__ == "__main__":
    datasets = load_datasets("../datatracer/datasets")

    solver = DataTracer.load('datatracer.primary_key.basic')
    for dataset_name, (metadata, tables) in datasets.items():
        print(dataset_name, evaluate_primary_key(solver, metadata, tables))

    solver = DataTracer.load('datatracer.foreign_key.standard')
    for dataset_name, (metadata, tables) in datasets.items():
        print(dataset_name, evaluate_foreign_key(solver, metadata, tables))
