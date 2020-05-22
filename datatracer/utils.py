from datatracer.dataset import Dataset, Field, Table


def get_tables_list(data):
    if isinstance(data, Table):
        return [data]
    elif isinstance(data, Dataset):
        return list(data.tables.values())
    else:
        return [
            table
            for element in data
            for table in get_tables_list(element)
        ]


def get_fields_list(data):
    if isinstance(data, Field):
        return [data]
    else:
        return [
            field
            for table in get_tables_list(data)
            for field in table.fields.values()
        ]


def by_dataset(wrapped):
    def wrapper(data, *args, **kwargs):
        if isinstance(data, Dataset):
            datasets = [data]
        else:
            datasets = data

        results = list()
        for dataset in datasets:
            results.append(wrapped(dataset, *args, **kwargs))

        return data

    wrapper.__doc__ = wrapped.__doc__
    wrapper.__name__ = wrapped.__name__

    return wrapper


def by_table(wrapped):
    def wrapper(data, *args, **kwargs):
        results = list()
        for table in get_tables_list(data):
            results.append(wrapped(table, *args, **kwargs))

        return data

    wrapper.__doc__ = wrapped.__doc__
    wrapper.__name__ = wrapped.__name__

    return wrapper


def by_field(wrapped):
    def wrapper(data, *args, **kwargs):
        results = list()
        for field in get_fields_list(data):
            results.append(wrapped(field, *args, **kwargs))

        return data

    wrapper.__doc__ = wrapped.__doc__
    wrapper.__name__ = wrapped.__name__

    return wrapper
