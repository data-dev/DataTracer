<p align="left">
<img width=15% src="https://dai.lids.mit.edu/wp-content/uploads/2018/06/Logo_DAI_highres.png" alt=“DAI-Lab” />
<i>An open source project from Data to AI Lab at MIT.</i>
</p>

[![Development Status](https://img.shields.io/badge/Development%20Status-2%20--%20Pre--Alpha-yellow)](https://pypi.org/search/?c=Development+Status+%3A%3A+2+-+Pre-Alpha)
[![PyPI Shield](https://img.shields.io/pypi/v/datatracer.svg)](https://pypi.python.org/pypi/datatracer)
[![Downloads](https://pepy.tech/badge/datatracer)](https://pepy.tech/project/datatracer)
[![Run Tests](https://github.com/data-dev/DataTracer/workflows/Run%20Tests/badge.svg)](https://github.com/data-dev/DataTracer/actions)

# DataTracer

Data Lineage Tracing Library

* License: [MIT](https://github.com/data-dev/DataTracer/blob/master/LICENSE)
* Development Status: [Pre-Alpha](https://pypi.org/search/?c=Development+Status+%3A%3A+2+-+Pre-Alpha)
* Homepage: https://github.com/data-dev/DataTracer

## Overview

DataTracer is a Python library for solving Data Lineage problems using  statistical
methods, machine learning techniques, and hand-crafted heuristics.

Currently the Data Tracer library implements discovery of the following properties:

* **Primary Key**: Identify which column is the primary key in each table.
* **Foreign Key**: Find which relationships exist between the tables.
* **Column Mapping**: Given a field in a table, deduce which other fields, from the same table
  or other tables, are more related or contributed the most in generating the given field.

# Install

## Requirements

**DataTracer** has been developed and tested on [Python 3.5 and 3.6, 3.7](https://www.python.org/downloads/)

Also, although it is not strictly required, the usage of a [virtualenv](
https://virtualenv.pypa.io/en/latest/) is highly recommended in order to avoid
interfering with other software installed in the system where **DataTracer** is run.

## Install with pip

The easiest and recommended way to install **DataTracer** is using [pip](
https://pip.pypa.io/en/stable/):

```bash
pip install datatracer
```

This will pull and install the latest stable release from [PyPi](https://pypi.org/).

If you want to install from source or contribute to the project please read the
[Contributing Guide](https://hdi-project.github.io/DataTracer/contributing.html#get-started).


# Data Format: Datasets and Metadata

The DataTracer library is prepared to work using datasets, which are a collection of tables
loaded as `pandas.DataFrames` and a MetaData JSON which provides information about the
dataset structure.

You can find more information about the MetaData format in the [MetaData repository](
https://github.com/signals-dev/MetaData).

The DataTracer also includes a few [demo datasets](datatracer/datasets) which you can easily
download to your computer using the `datatracer.get_demo_data` function:

```python3
from datatracer import get_demo_data

get_demo_data()
```

This will create a folder called `datatracer_demo` in your working directory with a few
datasets ready to use inside it.

# Quickstart

In this short tutorial we will guide you through a series of steps that will help you
getting started with **Data Tracer**.

## Load data

The first step will be to load the data in the format expected by DataTracer.

For this, we can use the `datatracer.load_datasets`  function passing the path to
where we have our datasets.

For example, if we are using the demo datasets, we can load them using:

```python3
from datatracer import load_datasets

datasets = load_datasets('datatracer_demo')
```

This will return a dictionary of dataset names and tuples, each one of them containing:

* A `MetaData` instance with details about the dataset.
* A `dict` with all the tables of the dataset loaded as a `pandas.DataFrame`.

For the rest of the tutorial, we will use the dataset called `classicalmodels`
for our testing, and use the rest of the datasets to train the DataTracer.

```python3
metadata, tables = datasets.pop('classicmodels')
```

## Select a Pipeline

In the DataTracer project, the different Data Lineage problems are solved using what we
call _pipelines_.

We can see the list of available pipelines using the `get_pipelines` function:

```python3
from datatracer import get_pipelines

get_pipelines()
```

This will return a list with the names of the available pipelines:

```
['datatracer.column_map',
 'datatracer.detection.primary',
 'datatracer.foreign_key.basic',
 'datatracer.foreign_key.standard',
 'datatracer.primary_key.basic']
```

## Use a DataTracer instance to find table relationships

In order to use a pipeline you will need to create a `DataTracer` instance passing the name of
the pipeline that we want to use.

In this example, we will try to figure out the relationships between the tables in our dataset
by using the pipeline `datatracer.foreign_key.standard`.

```python3
from datatracer import DataTracer

# Create the DataTrace instance
dtr = DataTracer('datatracer.foreign_key.standard')

# Fit it to our training datasets
dtr.fit(datasets)

# Solve the Data Lineage problem
foreign_keys = dtr.solve(tables)
```

The result will be a dictionary containing the foreign key candidates:

```
[{'table': 'products',
  'field': 'productLine',
  'ref_table': 'productlines',
  'ref_field': 'productLine'},
 {'table': 'payments',
  'field': 'customerNumber',
  'ref_table': 'customers',
  'ref_field': 'customerNumber'},
 {'table': 'orders',
  'field': 'customerNumber',
  'ref_table': 'customers',
  'ref_field': 'customerNumber'},
 {'table': 'orderdetails',
  'field': 'productCode',
  'ref_table': 'products',
  'ref_field': 'productCode'},
 {'table': 'orderdetails',
  'field': 'orderNumber',
  'ref_table': 'orders',
  'ref_field': 'orderNumber'},
 {'table': 'employees',
  'field': 'officeCode',
  'ref_table': 'offices',
  'ref_field': 'officeCode'}]
```
