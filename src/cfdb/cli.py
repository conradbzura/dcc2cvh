import asyncio
import csv
import json
import logging
import os
import urllib.parse
from contextlib import asynccontextmanager
from copy import copy
from typing import Final

import aiohttp
import click
from pymongo import MongoClient

from cfdb.mongo import PIPELINE


BATCH_SIZE: Final = 1000

__client__ = None
__aiohttp_session__ = None

logging.basicConfig(level=logging.INFO)


class TTLSemaphore(asyncio.BoundedSemaphore):
    def __init__(self, limit: int, interval: float):
        super().__init__(limit)
        self._interval = interval

    async def __aexit__(self, *args, **kwargs):
        pass

    async def acquire(self):
        await super().acquire()
        asyncio.get_event_loop().call_later(self._interval, self.release)
        return True


_request_semaphore = TTLSemaphore(50, 1)


@asynccontextmanager
async def rate_limit():
    await _request_semaphore.acquire()
    yield


def debug(ctx, param, value):
    """
    Enable debugging with debugpy.

    Args:
        ctx (click.Context): Click context.
        param (click.Parameter): Click parameter.
        value (bool): Flag value indicating whether to enable debugging.
    """
    if not value or ctx.resilient_parsing:
        return

    import debugpy

    debugpy.listen(5678)
    logging.debug("Waiting for debugger to attach...")
    debugpy.wait_for_client()
    logging.debug("Debugger attached")


def get_client(port=27017):
    global __client__
    if not __client__:
        __client__ = MongoClient(f"mongodb://localhost:{port}/")
    return __client__


async def get_aiohttp_session():
    """Get or create a reusable aiohttp ClientSession for DRS metadata fetching."""
    global __aiohttp_session__
    if not __aiohttp_session__:
        __aiohttp_session__ = aiohttp.ClientSession()
        logging.debug("Created new aiohttp ClientSession")
    return __aiohttp_session__


async def close_aiohttp_session():
    """Close the aiohttp ClientSession if it exists."""
    global __aiohttp_session__
    if __aiohttp_session__:
        await __aiohttp_session__.close()
        __aiohttp_session__ = None
        logging.debug("Closed aiohttp ClientSession")


@click.group()
def cli(): ...


async def _load_cfdb_dataset(directory: os.PathLike, db_name: str = "cfdb"):
    logging.debug(f"Loading directory: {directory} into database: {db_name}")
    try:
        tasks = []
        files_to_load = [
            f for f in os.listdir(directory) if f.endswith((".csv", ".tsv"))
        ]
        logging.info(f"Loading {len(files_to_load)} files into {db_name}")

        for filename in files_to_load:
            delimiter = "," if filename.endswith(".csv") else "\t"
            filepath = os.path.join(directory, filename)
            logging.debug(f"Creating persist task for {filename}")
            # Disable DRS metadata fetching by default - too slow for 3M+ file records
            tasks.append(
                asyncio.create_task(
                    persist(filepath, delimiter, db_name, fetch_drs_metadata=False)
                )
            )

        logging.info(f"Starting to load {len(tasks)} tables into MongoDB")
        files_list = ", ".join(files_to_load)
        logging.info(f"Files to load: {files_list}")

        try:
            # Use a timeout to detect hangs - 5 minutes per table should be plenty
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=300
            )
            logging.info("All tables loaded successfully")
        except asyncio.TimeoutError:
            logging.error(
                "Timeout waiting for tables to load - some tasks are still running"
            )
            # Get results of completed tasks to see which one is hanging
            done, pending = await asyncio.wait(tasks, timeout=1)
            completed_count = len(done)
            pending_count = len(pending)
            logging.error(
                f"Completed: {completed_count}/{len(tasks)} tasks, Pending: {pending_count}"
            )
            raise

        # Check for errors
        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            logging.warning(f"Encountered {len(errors)} errors during loading")
            for error in errors:
                logging.warning(f"Error: {error}")

        return results
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
        raise
    finally:
        # Clean up aiohttp session
        await close_aiohttp_session()


async def persist(
    filepath, delimiter, db_name: str = "cfdb", fetch_drs_metadata: bool = False
):
    """
    Persist records from a CSV/TSV file to MongoDB.

    Args:
        filepath: Path to the CSV/TSV file
        delimiter: Field delimiter (',' for CSV, '\\t' for TSV)
        db_name: MongoDB database name
        fetch_drs_metadata: Whether to fetch and validate DRS metadata for file records.
                          NOT recommended for large datasets (e.g., HuBMAP with 3M files).
                          If False, stores access_url as-is without validation.
    """
    try:
        logging.debug(f"Reading file: {filepath}")
        count = 0
        with open(filepath, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter=delimiter)
            *_, directory, filename = os.path.split(filepath)
            directory = os.path.basename(directory)
            table, _ = os.path.splitext(filename)
            logging.info(
                f"Persisting table: {table} from {filename} (fetch_drs_metadata={fetch_drs_metadata})"
            )
            batch = []
            metadata_tasks = []  # List of (index, task) tuples to track which records need metadata
            for row in reader:
                count += 1
                record = {**row, "submission": directory, "table": table}

                # Mark 4DN files as public
                if directory == "4dn" and table == "file":
                    record["data_access_level"] = "public"

                batch.append(record)
                if (
                    fetch_drs_metadata
                    and table == "file"
                    and (access_url := record.get("access_url"))
                ):
                    metadata_task = asyncio.create_task(get_file_metadata(access_url))
                    # Track index in batch and the metadata task
                    metadata_tasks.append((len(batch) - 1, metadata_task))
                if len(batch) >= BATCH_SIZE:
                    logging.debug(
                        f"  Processing batch of {len(batch)} records (total {count} read so far), with {len(metadata_tasks)} metadata tasks"
                    )
                    # Await all metadata tasks and mark records with valid_drs flag
                    if metadata_tasks:
                        logging.debug(
                            f"    Awaiting {len(metadata_tasks)} metadata tasks..."
                        )
                        try:
                            metadata_results = await asyncio.gather(
                                *[t for _, t in metadata_tasks], return_exceptions=True
                            )
                            for (batch_idx, _), metadata in zip(
                                metadata_tasks, metadata_results
                            ):
                                batch[batch_idx]["valid_drs"] = (
                                    metadata is not None
                                    and not isinstance(metadata, Exception)
                                )
                            logging.debug("Metadata fetch complete")
                        except Exception as e:
                            logging.error(
                                f"Error gathering metadata tasks: {e}", exc_info=True
                            )
                            raise

                    # Mark records based on whether metadata was fetched
                    if fetch_drs_metadata:
                        for record in batch:
                            if "valid_drs" not in record:
                                record["valid_drs"] = (
                                    True  # No metadata attempt = assume valid
                                )

                    logging.debug(
                        f"    Creating flush task for batch of {len(batch)} records"
                    )
                    await flush(db_name, table, copy(batch))
                    batch.clear()
                    metadata_tasks.clear()
            if batch:
                logging.debug(
                    f"  Processing final batch of {len(batch)} records (total {count} read)"
                )
                # Await all metadata tasks and mark records
                if metadata_tasks:
                    logging.debug(
                        f"    Awaiting {len(metadata_tasks)} metadata tasks..."
                    )
                    try:
                        metadata_results = await asyncio.gather(
                            *[t for _, t in metadata_tasks], return_exceptions=True
                        )
                        for (batch_idx, _), metadata in zip(
                            metadata_tasks, metadata_results
                        ):
                            batch[batch_idx]["valid_drs"] = (
                                metadata is not None
                                and not isinstance(metadata, Exception)
                            )
                        logging.debug("Metadata fetch complete")
                    except Exception as e:
                        logging.error(
                            f"Error gathering metadata tasks: {e}", exc_info=True
                        )
                        raise

                # Mark remaining records based on whether metadata was fetched
                if fetch_drs_metadata:
                    for record in batch:
                        if "valid_drs" not in record:
                            record["valid_drs"] = True

                logging.debug(
                    f"    Creating flush task for final batch of {len(batch)} records"
                )
                await flush(db_name, table, copy(batch))
                metadata_tasks.clear()
                batch.clear()
        logging.info(f"Persisted {count} records to {db_name}.{table}")
    except asyncio.CancelledError:
        logging.warning("Cancelled!")
        raise


async def flush(db, table, batch):
    get_client()[db][table].insert_many(batch)
    logging.debug(f"Flushed {len(batch)} records to {db}.{table}")


def aggregate(db):
    yield from db.file.aggregate(PIPELINE)


@cli.command
def create_files_view():
    db = get_client()["cfdb-backup"]
    db.drop_collection("files")
    db.command(
        "create",
        "files",
        viewOn="file",
        pipeline=PIPELINE,
        writeConcern={"w": 1},
    )


def clear_dcc_data(db_name: str, submission: str):
    """
    Delete all records with matching submission field from all collections in a database.

    Given-When-Then:
    - Given a database name and submission identifier
    - When records with matching submission are deleted from all collections
    - Then deleted record counts are logged

    Args:
        db_name: MongoDB database name
        submission: Submission identifier to match
    """
    client = get_client()
    db = client[db_name]

    for collection_name in db.list_collection_names():
        try:
            result = db[collection_name].delete_many({"submission": submission})
            if result.deleted_count > 0:
                logging.info(
                    f"Deleted {result.deleted_count} records from {db_name}.{collection_name}"
                )
        except Exception as e:
            logging.warning(
                f"Failed to delete records from {db_name}.{collection_name}: {e}"
            )


@cli.command("sync")
@click.argument("dcc_names", nargs=-1, required=False)
@click.option("--backup", is_flag=True, help="Backup existing data before syncing")
@click.option("--keep-downloads", is_flag=True, help="Keep downloaded ZIP files")
@click.option("--data-dir", default=".data", type=click.Path(), help="Data directory")
@click.option("--db-name", default="cfdb", help="MongoDB database name")
@click.option(
    "--debug",
    "-d",
    callback=debug,
    expose_value=False,
    help="Run with debugger listening on the specified port. Execution will block until the debugger is attached.",
    is_eager=True,
    type=int,
)
def sync(
    dcc_names: tuple[str, ...],
    backup: bool,
    keep_downloads: bool,
    data_dir: str,
    db_name: str,
):
    """
    Download and load C2M2 datapackages from CFDE consortia.

    If no DCC_NAMES are specified, all supported DCCs will be synced.

    DCC_NAMES: Zero or more DCC names (4dn, hubmap). If omitted, all DCCs are synced.

    Examples:

        cfdb cfdb sync

        cfdb cfdb sync 4dn

        cfdb cfdb sync 4dn hubmap --backup
    """
    asyncio.get_event_loop().run_until_complete(
        _sync_dccs(dcc_names, backup, keep_downloads, data_dir, db_name)
    )


async def _sync_dccs(
    dcc_names: tuple[str, ...],
    backup: bool,
    keep_downloads: bool,
    data_dir: str,
    db_name: str,
):
    """
    Core sync implementation for downloading and loading C2M2 datapackages.

    Given-When-Then:
    - Given DCC names and sync options
    - When datapackages are downloaded, extracted, and loaded into MongoDB
    - Then status updates are logged and data is available for querying
    """
    import shutil
    from pathlib import Path

    from cfdb.dcc_registry import (
        get_all_dcc_names,
        get_dcc_config,
        normalize_dcc_name,
    )
    from cfdb.downloader import cleanup_zip, download_file, extract_zip

    try:
        data_path = Path(data_dir)
        data_path.mkdir(exist_ok=True)
        downloads_path = data_path / "downloads"
        downloads_path.mkdir(exist_ok=True)

        # Validate DCC names and use all DCCs if none specified
        valid_dccs = get_all_dcc_names()

        if not dcc_names:
            # No DCCs specified, use all supported DCCs
            normalized_dccs = valid_dccs
            click.echo(
                f"No DCCs specified. Syncing all supported DCCs: {', '.join(normalized_dccs)}\n"
            )
        else:
            # Validate specified DCC names
            normalized_dccs = []
            for dcc in dcc_names:
                normalized = normalize_dcc_name(dcc)
                if normalized not in valid_dccs:
                    click.echo(f"Error: Unknown DCC '{dcc}'", err=True)
                    click.echo(f"Available DCCs: {', '.join(valid_dccs)}")
                    raise click.Abort()
                normalized_dccs.append(normalized)

        # Sync each DCC
        for dcc in normalized_dccs:
            click.echo(f"\nSyncing {dcc.upper()}...")
            config = get_dcc_config(dcc)

            # 1. Download
            url = config["latest_url"]
            zip_filename = Path(url).name
            zip_path = downloads_path / zip_filename

            click.echo(f"Downloading from {url}...")
            try:
                await download_file(url, zip_path, show_progress=True)
            except Exception as e:
                click.echo(f"Error: Failed to download datapackage: {e}", err=True)
                raise

            # 2. Backup if requested
            extract_dir = data_path / dcc
            if backup and extract_dir.exists():
                backup_dir = data_path / f"{dcc}.bak"
                if backup_dir.exists():
                    logging.debug(f"Removing existing backup: {backup_dir}")
                    shutil.rmtree(backup_dir)
                click.echo(f"Backing up existing data to {backup_dir}...")
                shutil.move(str(extract_dir), str(backup_dir))

            # 3. Extract
            click.echo(f"Extracting to {extract_dir}...")
            try:
                extract_zip(zip_path, extract_dir)
            except Exception as e:
                click.echo(f"Error: Failed to extract datapackage: {e}", err=True)
                raise

            # 4. Clear existing data from MongoDB
            click.echo(f"Clearing existing {dcc} data from database...")
            clear_dcc_data(db_name, dcc)

            # 5. Load into MongoDB
            click.echo(f"Loading data into {db_name} database...")
            try:
                await _load_cfdb_dataset(extract_dir, db_name)
            except Exception as e:
                click.echo(f"Error: Failed to load data into MongoDB: {e}", err=True)
                raise

            # 6. Cleanup ZIP
            if not keep_downloads:
                cleanup_zip(zip_path)

            click.echo(f"✓ {dcc.upper()} synced successfully")

        click.echo("\nAll syncs complete!")
    finally:
        # Ensure session cleanup even if sync fails
        await close_aiohttp_session()


async def get_file_metadata(url):
    """
    Fetch file metadata from DRS URL using GA4GH DRS API.

    Given-When-Then:
    - Given a DRS URI (drs://hostname/object-id)
    - When the URI is converted to a GA4GH DRS API endpoint
    - Then JSON metadata is returned, or None if unavailable/failed

    Args:
        url: DRS URI (e.g., drs://drs.hubmapconsortium.org/object-id)

    Returns:
        Parsed JSON metadata dict from DRS API, or None if fetch/parse fails
    """
    try:
        # Parse DRS URI: drs://hostname/object-id
        parsed_url = urllib.parse.urlparse(url)
        if parsed_url.scheme != "drs":
            logging.debug(f"Skipping non-DRS URL: {url}")
            return None

        # Extract hostname and object ID from DRS URI
        hostname = parsed_url.netloc
        object_id = parsed_url.path.lstrip("/")

        if not hostname or not object_id:
            logging.warning(f"Invalid DRS URI format: {url}")
            return None

        # Construct GA4GH DRS API endpoint
        drs_api_url = f"https://{hostname}/ga4gh/drs/v1/objects/{object_id}"

        session = await get_aiohttp_session()
        logging.debug(f"Fetching DRS metadata from {drs_api_url}")
        async with rate_limit():
            async with session.get(
                drs_api_url, timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    metadata = await response.json()
                    logging.debug(
                        f"Retrieved metadata for {object_id}: {metadata.get('name')} ({metadata.get('size')} bytes)"
                    )
                    return metadata
                elif response.status == 404:
                    # File doesn't exist in DRS - this is normal for some C2M2 entries
                    # Files may be deleted, not yet indexed, or stale references
                    logging.debug(
                        f"DRS object not found: {object_id} (HTTP 404). "
                        "This file may have been deleted, not yet indexed, or is a stale reference in C2M2."
                    )
                    return None
                else:
                    logging.warning(
                        f"Failed to fetch DRS metadata from {drs_api_url}: HTTP {response.status}"
                    )
                    return None
    except asyncio.TimeoutError:
        logging.debug(
            f"Timeout fetching DRS metadata from {url} (expected for unreachable files)"
        )
        return None
    except aiohttp.ClientError as e:
        logging.debug(f"Network error fetching DRS metadata from {url}: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.warning(f"Failed to parse JSON from DRS endpoint for {url}: {e}")
        return None
    except Exception as e:
        logging.warning(f"Unexpected error fetching DRS metadata from {url}: {e}")
        return None


if __name__ == "__main__":
    cli()
