"""Download and extraction utilities for C2M2 datapackages."""

import asyncio
import logging
import shutil
import zipfile
from pathlib import Path
from typing import Optional, Callable

import aiohttp
import click


async def download_file(
    url: str,
    destination: Path,
    show_progress: bool = True,
    max_retries: int = 3,
) -> Path:
    """
    Download a file from URL with progress indication and retry logic.

    Given-When-Then:
    - Given a remote URL and local destination path
    - When the file is downloaded with optional progress display
    - Then the file is saved and the path is returned

    Args:
        url: URL to download from
        destination: Path where file should be saved
        show_progress: Whether to show progress bar
        max_retries: Maximum number of retry attempts

    Returns:
        Path to the downloaded file

    Raises:
        aiohttp.ClientError: If download fails after all retries
        IOError: If file cannot be written
    """
    destination.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=f"HTTP {response.status}",
                            headers=response.headers,
                        )

                    total_size = response.content_length or 0
                    downloaded = 0

                    with open(destination, "wb") as f:
                        async for chunk in response.content.iter_chunked(8192):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)

                                if show_progress and total_size > 0:
                                    percent = (downloaded / total_size) * 100
                                    mb = downloaded / 1024 / 1024
                                    total_mb = total_size / 1024 / 1024
                                    click.secho(
                                        f"\r  [{percent:5.1f}%] {mb:.1f} MB / {total_mb:.1f} MB",
                                        err=True,
                                        nl=False,
                                    )

            # Print newline after progress bar completes
            if show_progress:
                click.echo()
            logging.info(f"Downloaded {destination.name}")
            return destination

        except (aiohttp.ClientError, OSError) as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logging.warning(
                    f"Download failed (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {wait_time}s: {e}"
                )
                await asyncio.sleep(wait_time)
            else:
                logging.error(f"Download failed after {max_retries} attempts: {e}")
                raise


def extract_zip(zip_path: Path, extract_dir: Path) -> Path:
    """
    Extract ZIP archive to directory.

    Given-When-Then:
    - Given a ZIP file path and target extraction directory
    - When the ZIP is extracted and integrity is validated
    - Then the extraction directory is returned

    Args:
        zip_path: Path to ZIP file
        extract_dir: Directory where ZIP should be extracted

    Returns:
        Path to extraction directory

    Raises:
        zipfile.BadZipFile: If ZIP file is corrupted
        IOError: If extraction fails
    """
    # Clear extraction directory if it exists
    if extract_dir.exists():
        logging.debug(f"Removing existing extraction directory: {extract_dir}")
        shutil.rmtree(extract_dir)

    extract_dir.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            # Validate ZIP integrity
            bad_file = zf.testzip()
            if bad_file:
                raise zipfile.BadZipFile(f"Corrupted file in archive: {bad_file}")

            # Extract all files
            zf.extractall(extract_dir)
            logging.info(f"Extracted {len(zf.namelist())} files to {extract_dir}")

    except zipfile.BadZipFile as e:
        logging.error(f"ZIP file is corrupted: {e}")
        # Clean up the corrupted extraction
        if extract_dir.exists():
            shutil.rmtree(extract_dir)
        raise

    return extract_dir


def cleanup_zip(zip_path: Path) -> None:
    """
    Delete a ZIP file after extraction.

    Given-When-Then:
    - Given a ZIP file path
    - When the file is deleted
    - Then no errors are raised if file doesn't exist

    Args:
        zip_path: Path to ZIP file to delete
    """
    try:
        zip_path.unlink()
        logging.debug(f"Deleted {zip_path}")
    except FileNotFoundError:
        logging.debug(f"File not found (already deleted): {zip_path}")
    except Exception as e:
        logging.warning(f"Failed to delete {zip_path}: {e}")
