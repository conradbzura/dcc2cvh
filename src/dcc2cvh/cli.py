import csv
import os

import click
from pymongo import MongoClient

from dcc2cvh.mongo import PIPELINE

__client__ = None


def get_client(port=27017):
    global __client__
    if not __client__:
        __client__ = MongoClient(f"mongodb://localhost:{port}/")
    return __client__


@click.group()
def cli(): ...


@cli.group("c2m2")
def c2m2(): ...


@c2m2.command("load-dataset")
@click.argument(
    "directory",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
)
def load_c2m2_dataset(directory: os.PathLike):
    """
    Reads and processes a CFDE datapackage.

    This command processes all `.csv` and `.tsv` files in the specified directory,
    reads their contents, and pushes the data to a MongoDB database.

    DIRECTORY is the path to the directory containing the CFDE datapackage.
    """
    click.echo(f"Loading directory: {directory}")
    for filename in os.listdir(directory):
        if filename.endswith(".csv") or filename.endswith(".tsv"):
            delimiter = "," if filename.endswith(".csv") else "\t"
            filepath = os.path.join(directory, filename)
            persist(filepath, delimiter)


def persist(filepath, delimiter):
    click.echo(f"Reading file: {filepath}")
    count = 0
    with open(filepath, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter=delimiter)
        *_, directory, filename = os.path.split(filepath)
        directory = os.path.basename(directory)
        table, _ = os.path.splitext(filename)
        click.echo(f"Persisting table: {table}")
        batch = []
        batch_size = 1000
        for row in reader:
            count += 1
            record = {**row, "submission": directory, "table": table}
            batch.append(record)
            if len(batch) >= batch_size:
                flush("c2m2", table, batch)
        if batch:
            flush("c2m2", table, batch)
    click.echo(f"Persisted {count} records to c2m2.{table}")


def flush(db, table, batch):
    get_client()[db][table].insert_many(batch)
    click.echo(f"Flushed {len(batch)} records to {db}.{table}")
    batch.clear()


def aggregate(db):
    yield from db.file.aggregate(PIPELINE)


if __name__ == "__main__":
    cli()
