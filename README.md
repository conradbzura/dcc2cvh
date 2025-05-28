# dcc2cvh

## Overview
`dcc2cvh` is a Python package designed to process CFDE and ENCODE datapackages and push the data to a MongoDB database. It provides a CLI tool for reading `.csv` and `.tsv` files from a specified directory and loading them into MongoDB.

## Installation
To install the package from GitHub, use the following command:

```bash
pip install git+https://github.com/conradbzura/dcc2cvh.git
```

## Starting MongoDB
This project includes a `Makefile` to simplify building and starting MongoDB and the API. To start MongoDB, run:

```bash
make mongodb
make api
```

This will start a container running MongoDB listening on port 27017 and the GraphQL API listening on port 8000.

> [!NOTE]
Ensure that both Docker is installed on your system and properly configured before running this command.

## Using the CLI
The package provides a CLI command `load-dataset` under the command group `c2m2` to process and load CFDE datapackages into MongoDB.

### Command: `load-dataset`

#### Usage
```bash
dcc2cvh c2m2 load-dataset DIRECTORY
```

#### Arguments
- `DIRECTORY`: The path to the directory containing the CFDE datapackage. This directory should contain `.csv` or `.tsv` files.
