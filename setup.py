#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md', encoding='utf-8') as readme_file:
    readme = readme_file.read()

with open('HISTORY.md', encoding='utf-8') as history_file:
    history = history_file.read()

install_requires = [
    'boto3>=1.13,<2',
    'pandas>=0.23.4,<0.25',
    'scikit-learn>=0.20.0,<0.21',
    'numpy<1.17,>=1.15.2',
    'mlblocks==0.3.4',
    'metad==0.0.2',
    'falcon>=2.0.0,<3',
    'hug>=2.6.1,<3',
    'pyyaml>=5.3.1,<6',
    'tqdm>=4,<5',
]

setup_requires = [
    'pytest-runner>=2.11.1',
]

tests_require = [
    'pytest>=3.4.2',
    'pytest-cov>=2.6.0',
    'jupyter>=1.0.0,<2',
    'rundoc>=0.4.3,<0.5',
]

development_requires = [
    # general
    'bumpversion>=0.5.3,<0.6',
    'pip>=9.0.1',
    'watchdog>=0.8.3,<0.11',

    # docs
    'm2r>=0.2.0,<0.3',
    'nbsphinx>=0.5.0,<0.7',
    'Sphinx>=1.7.1,<3',
    'sphinx_rtd_theme>=0.2.4,<0.5',
    'autodocsumm>=0.1.10',

    # style check
    'flake8>=3.7.7,<4',
    'isort>=4.3.4,<5',

    # fix style issues
    'autoflake>=1.1,<2',
    'autopep8>=1.4.3,<2',

    # distribute on PyPI
    'twine>=1.10.0,<4',
    'wheel>=0.30.0',

    # Advanced testing
    'coverage>=4.5.1,<6',
    'tox>=2.9.1,<4',

    # benchmarking
    'dask>=2.15,<3',
    'distributed>=2.15,<3',
]

setup(
    author='MIT Data To AI Lab',
    author_email='dailabmit@gmail.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description='Data Lineage Tracing Library',
    entry_points = {
        'mlblocks': [
            'primitives=datatracer:MLBLOCKS_PRIMITIVES',
            'pipelines=datatracer:MLBLOCKS_PIPELINES'
        ],
        'console_scripts': [
            'datatracer=datatracer.__main__:main',
            'datatracer-benchmark=benchmark.benchmark:main'
        ],
    },
    extras_require={
        'test': tests_require,
        'dev': development_requires + tests_require,
    },
    install_package_data=True,
    install_requires=install_requires,
    license='MIT license',
    long_description=readme + '\n\n' + history,
    long_description_content_type='text/markdown',
    include_package_data=True,
    keywords='datatracer data-tracer Data Tracer',
    name='datatracer',
    packages=find_packages(include=['datatracer', 'datatracer.*']),
    python_requires='>=3.5',
    setup_requires=setup_requires,
    test_suite='tests',
    tests_require=tests_require,
    url='https://github.com/HDI-Project/DataTracer',
    version='0.0.7.dev0',
    zip_safe=False,
)
