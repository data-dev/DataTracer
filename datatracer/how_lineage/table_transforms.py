def transform(tables, fk, col_name, op):
    child_table = tables[fk["table"]].copy()
    parent_table = tables[fk["ref_table"]].copy()
    child_table["_dummy_"] = 0.0
    if len(child_table.columns) <= 1:
        raise ValueError("Invalid lineage table/transformation combo!")
    result_col = op(child_table.groupby(fk["field"]))[col_name].rename('_target_')
    parent_table = parent_table.set_index(fk["ref_field"])
    parent_table = parent_table.join(result_col).reset_index()

    return parent_table['_target_'].fillna(0.0).values


def entries_count(tables, fk, col_name):
    return transform(tables, fk, '_dummy_', lambda x: x.count())


def entries_sum(tables, fk, col_name):
    return transform(tables, fk, col_name, lambda x: x.sum())


def entries_min(tables, fk, col_name):
    return transform(tables, fk, col_name, lambda x: x.min())


def entries_max(tables, fk, col_name):
    return transform(tables, fk, col_name, lambda x: x.max())


def entries_std(tables, fk, col_name):
    return transform(tables, fk, col_name, lambda x: x.std())


def columns_sum(columns):
    return sum(columns)


def columns_diff(columns):
    return columns[0] - columns[1]


def columns_avg(columns):
    return sum(columns) / len(columns)
