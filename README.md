# dcc2cvh

## Overview
`dcc2cvh` is a Python package designed to process CFDE and ENCODE datapackages and push the data to a MongoDB database. It provides a CLI tool for reading `.csv` and `.tsv` files from a specified directory and loading them into MongoDB.

## Installation
To install the package from GitHub, use the following command:

```bash
pip install git+https://github.com/conradbzura/dcc2cvh.git
```

## Starting MongoDB
This project includes a `Makefile` to simplify starting MongoDB. To start MongoDB, run:

```bash
make mongo
```

This will start a container running MongoDB listening on port 27017.

> [!NOTE]
Ensure that both Docker and MongoDB are installed on your system and properly configured before running this command.

## Using the CLI
The package provides a CLI command `load-c2m2-datapackage` to process and load CFDE datapackages into MongoDB.

### Command: `load-c2m2-datapackage`

#### Usage
```bash
load-c2m2-datapackage DIRECTORY
```

#### Arguments
- `DIRECTORY`: The path to the directory containing the CFDE datapackage. This directory should contain `.csv` or `.tsv` files.
