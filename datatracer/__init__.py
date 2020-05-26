# -*- coding: utf-8 -*-

"""Top-level package for DataTracer."""

__author__ = 'MIT Data To AI Lab'
__email__ = 'dailabmit@gmail.com'
__version__ = '0.0.2'

import os

from mlblocks import discovery

from datatracer.core import DataTracer
from datatracer.data import get_demo_data, load_dataset, load_datasets

_BASE_PATH = os.path.abspath(os.path.dirname(__file__))
_JSONS_PATH = os.path.join(_BASE_PATH, 'jsons')
MLBLOCKS_PRIMITIVES = os.path.join(_JSONS_PATH, 'primitives')
MLBLOCKS_PIPELINES = os.path.join(_JSONS_PATH, 'pipelines')


__all__ = (
    'DataTracer',
    'get_demo_data',
    'get_pipelines',
    'get_primitives',
    'load_dataset',
    'load_datasets',
)


def get_pipelines():
    """Get a list of the available datatracer pipelines.

    Returns:
        list:
            List of the names of the available datatracer pipelines.
    """
    return discovery.find_pipelines('datatracer')


def get_primitives():
    """Get a list of the available datatracer primitives.

    Returns:
        list:
            List of the names of the available datatracer primitives.
    """
    return discovery.find_primitives('datatracer')
