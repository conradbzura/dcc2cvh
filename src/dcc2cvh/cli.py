import os
import csv

import click
from pymongo import MongoClient


__client__ = None


def get_client(port=27017):
    global __client__
    if not __client__:
        __client__ = MongoClient(f"mongodb://localhost:{port}/")
    return __client__


@click.group()
def cli():
    pass


@cli.command()
@click.argument(
    "directory",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
)
def load_c2m2_datapackage(directory: os.PathLike):
    """
    Reads and processes a CFDE datapackage.

    This command processes all `.csv` and `.tsv` files in the specified directory,
    reads their contents, and pushes the data to a MongoDB database.

    DIRECTORY is the path to the directory containing the CFDE datapackage.
    """
    for filename in os.listdir(directory):
        if filename.endswith(".csv") or filename.endswith(".tsv"):
            delimiter = "," if filename.endswith(".csv") else "\t"
            filepath = os.path.join(directory, filename)
            read(filepath, delimiter)


def read(filepath, delimiter):
    click.echo(f"Reading file: {filepath}")
    with open(filepath, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter=delimiter)
        *_, directory, filename = os.path.split(filepath)
        directory = os.path.basename(directory)
        table, _ = os.path.splitext(filename)
        click.echo(f"Table: {table}")
        click.echo(f"Directory: {directory}")
        batch = []
        batch_size = 1000
        for row in reader:
            record = {**row, "submission": directory}
            batch.append(record)
            if len(batch) >= batch_size:
                flush("c2m2", table, batch)
            batch = []
        if batch:
            flush("c2m2", table, batch)


def flush(db, table, batch):
    get_client()[db][table].insert_many(batch)


if __name__ == "__main__":
    cli()
