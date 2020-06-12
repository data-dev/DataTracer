# REST API

The DataTracer library incorporates a REST API that enables interaction with the DataTracer
Solvers via HTTP communication.

## Development Status

The current version of this REST api supports the following functionalities:

* **Primary Key Detection**
* **Foreign Key Detection**
* **Column Mapping**
* **Update Metadata**

## Using the REST API Server

Starting the REST API locally in development mode is as simple as executing a single command.

Once you have installed `datatracer` in your system, just open a console and run:

```bash
datatracer api
```

You will see a message similar to this in your console:

```
################################
Starting the DataTracer REST API
################################
Serving on 127.0.0.1:8000...
```

After this point, the application is ready to start receiving HTTP requests at port 8000

### API Configuration

The DataTracer API has a few arguments that can be tweaked by writing a YAML configuration
file and passing it to the `datatracer api` call.

The configurable options are:

* `host`: IP address at which the API will be listening.
* `port`: Port address at which the API will be listening.
* `primary_key_solver`: Name of the solver to use for primary key detection.
* `foreign_key_solver`: Name of the solver to use for foreign key detection.
* `column_map_solver`: Name of the solver to use for column mapping.
* `column_map_threshold`: Minimum relevance required to include a field in the column mapping.

You will find an example of such a YAML file with the default values already set in this same
directory: [config.yml](config.yml)

If you want to modify any of these values edit the file and then make sure to pass the file
to the `datatracer api` command as follows:

```bash
datatracer api -c config.yml
```

## API Endpoints

### NOTE: Table Specifications and Table Identifiers

Over the endpoints description we will be mentioning the concepts _table
specification_ and _table identifier_.

#### Table Specification

A _table specification_ consists of a dictionary that contains at least one
of these two fields:
* `path`: Path to a CSV file
* `data`: CSV data passed as a string.

And then one or more additional fields that uniquely identify this table,
such as `id`, `name`, `system`...

This is an example of a _table specification_ that uses a `path`:

```json
{
    "path": "path/to/my.csv",
    "id": 1234,
    "name": "my_table",
    "system": "my_system"
}
```

And this is another example that uses `data` instead of `path`:

```json
{
    "data": "a_field,another_field\n1,4\n2,5\n3,6\n",
    "id": 4567,
    "name": "my_other_table",
}
```

#### Table Identifier

A _table identifier_ consists of a dictionary that is the same as the
corresponding _table specification_ but with the `data` field removed.

When working with multiple _table specifications_, the _table identifier_
uniquely identifies one of them.

For example, these would be valid _table identifiers_ for the _table
specifications_ showed above:

```json
[
    {
        "path": "path/to/my.csv",
        "id": 1234,
        "name": "my_table",
        "system": "my_system"
    },
    {
        "id": 4567,
        "name": "my_other_table"
    }
]
```

#### Usage within the API

_Table specifications_ will be used in the API _inputs_ every time data
needs to be passed, and _table identifiers_ will be always used in the
API responses to refer at the corresponding inputted table, as well as in
some requests, like `column_mapping`, which need to refer to one of the
inputted tables somewhere else in the request.

### PRIMARY KEY DETECTION: `POST /primary_key_detection/`

This endpoint uses a pre-trained DataTracer solver to solve a Primary
Key detection problem.

The input to this endpoint is just a field called `tables` which contains
a list of _table specifications_.

The output is a field called `primary_keys` which contains the corresponding
_table identifiers_ with the  `primary_key` field added to each one of them.

#### Primary Key Detection Input Example:

```json
{
    "tables": [
        {
            "id": 1234,
            "name": "a_table",
            "path": "a/path/to/a.csv"
        },
        {
            "id": 4567,
            "name": "another_table",
            "path": "a/path/to/another.csv"
        },
    ],
}
```

#### Primary Key Detection Output Example:

```json
{
    "primary_keys": [
        {
            "id": 1234,
            "name": "a_table",
            "path": "a/path/to/another.csv",
            "primary_key": "a_field_name"
        },
        {
            "id": 4567,
            "name": "another_table",
            "path": "a/path/to/a.csv",
            "primary_key": "another_field_name"
        },
    ],
}
```

### FOREIGN KEY DETECTION: `POST /foreign_key_detection/`

This endpoint uses a pre-trained DataTracer solver to solve a Foreign
Key detection problem.

The input to this endpoint is just a field called `tables` which contains
a list of _table specifications_.

The output is a field called `foreign_keys` which contains a list of
_foreign key specifications_.

Each _foreign key specification_ consists of a dictionary that contains four
fields:
* `table`: a _table identifier_
* `field`: The name of the field which is the foreign key in the given `table`.
* `ref_table`: the _table identifier_ of the table which is referenced by the foreign key.
* `ref_field`: The name of the field which is referenced by the foreign key.

#### Foreign Key Detection Input Example:

```json
{
    "tables": [
        {
            "id": 1234,
            "name": "a_table",
            "path": "a/path/to/a.csv"
        },
        {
            "id": 4567,
            "name": "another_table",
            "path": "a/path/to/another.csv"
        },
    ],
}
```

#### Foreign Key Detection Output Example:

```json
{
    "foreign_keys": [
        {
            "table": {
                "id": 4567,
                "name": "another_table",
                "path": "a/path/to/another.csv"
            },
            "field": "another_field",
            "ref_table": {
                "id": 1234,
                "name": "a_table",
                "path": "a/path/to/a.csv"
            },
            "ref_field": "a_field_name"
        }
    ]
}
```

### COLUMN MAPPING: `POST /column_mapping/`

This endpoint uses a pre-trained DataTracer solver to solve a Column Mapping problem.

The input to this endpoint contains three fields:
* `tables`: list of _table specifications_
* `target_table`: _table identifier_ that uniquely identifies one of the passed `tables`.
* `target_field`: name of one of the fields in the given `target_table`.

The output contains three fields:
* `target_table`: _table identifier_ of the table that is being analyzed.
* `target_field`: Name of the field that is being analyzed.
* `column_mappings`: list of _column mapping specifications_.

Each _column mapping specification_ consists of a dictionary that contains
three fields:
* `table`: a _table identifier_
* `field`: The name of a field


#### Column Mapping Input Example:

```json
{
    "tables": [
        {
            "id": 1234,
            "name": "a_table",
            "path": "a/path/to/a.csv"
        },
        {
            "id": 4567,
            "name": "another_table",
            "path": "a/path/to/another.csv"
        },
    ],
    "target_table": {
        "id": 1234,
        "name": "a_table",
    },
    "target_field": "a_field",
}
```

#### Column Mapping Output Example:

```json
{
    "target_table": {
        "id": 1234,
        "name": "a_table",
        "path": "a/path/to/a.csv"
    },
    "target_field": "a_field",
    "column_mappings": [
        {
            "table": {
                "id": 4567,
                "name": "another_table",
                "path": "a/path/to/another.csv"
            },
            "field": "some_other_field"
        },
        {
            "table": {
                "id": 1234,
                "name": "a_table",
                "path": "a/path/to/a.csv"
            },
            "field": "some_field"
        },
    ]
}
```

### UPDATE METADATA: `POST /update_metadata/`

This endpoint updates a MetaData JSON applying the outputs from the `primary_key_detection`,
`foreign_key_detection` and `column_mapping` endpoints.

The input to this endpoint contains two fields:
* `metadata`: Contents of a MetaData JSON. Alternatively, a string containing the path
  to a MetaData JSON can also be passed.
* `update`: JSON specification of what two update. The contents of this JSON can be the
  outputs of any of the other endpoints. Combining all of them in a single JSON is also
  supported.

The output is the updated metadata.


#### Update Metadata Input Example:

**NOTE**: In this example we are updating the metadata adding a constraint based on the
column mapping output.

```json
{
    "metadata": {
        "tables": [
            {
                "id": 1234,
                "name": "a_table",
                "path": "a/path/to/a.csv",
                "fields": [
                    {
                        "name": "a_field",
                        "type": "number",
                        "subtype": "float"
                    },
                    {
                        "name": "some_field",
                        "type": "number",
                        "subtype": "float"
                    }
                ]
            },
            {
                "id": 4567,
                "name": "another_table",
                "path": "a/path/to/another.csv",
                "fields": [
                    {
                        "name": "another_field",
                        "type": "number",
                        "subtype": "float"
                    },
                    {
                        "name": "some_other_field",
                        "type": "number",
                        "subtype": "float"
                    }
                ]
            },
        ],
    },
    "update": {
        "target_table": {
            "id": 1234,
            "name": "a_table",
        },
        "target_field": "a_field",
        "column_mappings": [
            {
                "table": {
                    "id": 4567,
                    "name": "another_table",
                    "path": "a/path/to/another.csv"
                },
                "field": "some_other_field"
            },
            {
                "table": {
                    "id": 1234,
                    "name": "a_table",
                    "path": "a/path/to/a.csv"
                },
                "field": "some_field"
            },
        ]
    }
}
```

#### Update Metadata Output Example:

```json
{
    "tables": [
        {
            "id": 1234,
            "name": "a_table",
            "path": "a/path/to/a.csv",
            "fields": [
                {
                    "name": "a_field",
                    "type": "number",
                    "subtype": "float"
                },
                {
                    "name": "some_field",
                    "type": "number",
                    "subtype": "float"
                }
            ]
        },
        {
            "id": 4567,
            "name": "another_table",
            "path": "a/path/to/another.csv",
            "fields": [
                {
                    "name": "another_field",
                    "type": "number",
                    "subtype": "float"
                },
                {
                    "name": "some_other_field",
                    "type": "number",
                    "subtype": "float"
                }
            ]
        },
    ],
    "constraints": [
        {
            "constraint_type": "lineage",
            "fields_under_consideration": [
                {
                    "table": "a_table",
                    "field": "a_field"
                }
            ],
            "related_fields": [
                {
                    "table": "a_table",
                    "field": "some_field"
                },
                {
                    "table": "another_table",
                    "field": "some_other_field"
                }
            ]
        }
    ]
}
```

## Usage Example

In this section we will be showing a few examples of how to interact with the DataTracer REST API
using the Python [requests library](https://requests.readthedocs.io/en/master/).

Before we start, let's make sure that we have started an instance of the DataTracer API:

```bash
datatracer api
```

After the API has started, let's open an interactive Python session, such as a Jupyter notebook,
and start playing with it.

First we will make sure to have some data available to play with.

```python3
from datatracer import get_demo_data

get_demo_data(force=True)
```

Once we have generated the demo data, let's choose a dataset and prepare a list of its tables.

```python3
import os

from datatracer import load_dataset

dataset_path = 'datatracer_demo/classicmodels/'
metadata = load_dataset(dataset_path)[0]

tables = metadata.data['tables']

for table in tables:
    table["path"] = os.path.join(dataset_path, table['name'] + '.csv')
```

This will create a list of dictionaries that contain, among other things, the names of the tables
of the dataset and the paths to the corresponding CSV files:

```python
[{'name': 'productlines',
  'path': 'datatracer_demo/classicmodels/productlines.csv'},
 {'name': 'payments', 'path': 'datatracer_demo/classicmodels/payments.csv'},
 {'name': 'employees', 'path': 'datatracer_demo/classicmodels/employees.csv'},
 {'name': 'customers', 'path': 'datatracer_demo/classicmodels/customers.csv'},
 {'name': 'orders', 'path': 'datatracer_demo/classicmodels/orders.csv'},
 {'name': 'offices', 'path': 'datatracer_demo/classicmodels/offices.csv'},
 {'name': 'products', 'path': 'datatracer_demo/classicmodels/products.csv'},
 {'name': 'orderdetails',
  'path': 'datatracer_demo/classicmodels/orderdetails.csv'}]
```

With this list of tables we can start making requests to the DataTracer API.

Let's start by import the `requests` library and passing the tables to the `primary_key_detection`
endpoint:

```python3
import requests

primary_keys = requests.post(
    'http://localhost:8000/primary_key_detection',
    json={
        'tables': tables
    }
).json()
```

This will return a list of primary keys in the format indicated above.

Similarly, we can obtain the list of foreign key candidates:

```python3
foreign_keys = requests.post(
    'http://localhost:8000/foreign_key_detection',
    json={
        'tables': tables
    }
).json()
```

Finally, we can try to solve a column mapping problem to obtain the list of columns which are
more likely to have participated in the generation of one column which we are interested in.

For example, let's try to solve the column mapping problem for the `quantityInStock` column
from the `products` table:

```python3
column_mappings = requests.post(
    'http://localhost:8000/column_mapping',
    json={
        'tables': tables,
        'target_table': {
            'name': 'products'
        },
        'target_field': 'quantityInStock'
    }
).json()
```

The result will be a JSON indicating the column mapping for the requested field:

```python
{
    'target_table': {
        'name': 'products'
    },
    'target_field': 'quantityInStock',
    'column_mappings': [
        {
            'table': {
                'name': 'orderdetails',
                'path': 'datatracer_demo/classicmodels/orderdetails.csv'
            },
            'field': 'orderLineNumber'
        }
    ]
}
```

Once we have all of this, we can try to update the metadata JSON with the new contents
by passing each output to the `update_metadata` endpoint.

For example, let's add the column mappings obtained as a lineage constraint:

```python3
metadata_dict = metadata.data

metadata_dict = requests.post(
    'http://localhost:8000/update_metadata',
    json={
        'metadata': metadata_dict,
        'update': column_mappings
    }
).json()
```

After this, the `metadata_dict` variable will contain the metadata dictionary with a new
constraint entry indicating the relationships previously discovered by the column mapping
endpoint.
