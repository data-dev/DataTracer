from datatracer.utils import by_field, by_table


@by_field
def count_uniques(field):
    field.metadata['number_of_uniques'] = len(field.data.unique())


@by_table
def count_rows(table):
    table.metadata['number_of_rows'] = len(table.data)


_DATA_TYPES = {
    'i': ('numerical', 'integer'),
    'f': ('numerical', 'float'),
    'O': ('categorical', 'categorical'),
    'b': ('categorical', 'boolean'),
    'M': ('datetime', None),
}


@by_field
def infer_dtypes(field):
    dtype_kind = field.data.infer_objects().dtype.kind
    data_type = _DATA_TYPES.get(dtype_kind)

    if data_type:
        data_type, subtype = data_type
        metadata = field.metadata
        metadata['data_type'] = data_type
        metadata['subtype'] = subtype
