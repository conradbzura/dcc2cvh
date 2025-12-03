"""Globus Transfer integration for streaming HuBMAP files."""

import asyncio
import os
import uuid
from typing import AsyncGenerator
import logging

try:
    from globus_sdk import TransferClient, AccessTokenAuthorizer, TransferData
    GLOBUS_AVAILABLE = True
except ImportError:
    GLOBUS_AVAILABLE = False
    TransferClient = None
    AccessTokenAuthorizer = None
    TransferData = None

logger = logging.getLogger(__name__)

DEST_ENDPOINT = os.getenv("GLOBUS_ENDPOINT_ID")

if not DEST_ENDPOINT:
    logger.warning(
        "GLOBUS_ENDPOINT_ID environment variable not set. "
        "HuBMAP file streaming will not work."
    )


async def transfer_and_stream(
    source_endpoint: str,
    source_path: str,
    filename: str,
    access_token: str
) -> AsyncGenerator[bytes, None]:
    """
    Initiate Globus transfer and stream file as it downloads.

    Strategy:
    1. Create unique temp file path in /tmp
    2. Submit Globus transfer task
    3. Poll file size and stream chunks as they arrive
    4. Clean up temp file after streaming

    Args:
        source_endpoint: Globus endpoint UUID (HuBMAP)
        source_path: Path on source endpoint
        filename: Original filename for temp file naming
        access_token: Globus access token for authentication

    Yields:
        Chunks of file bytes

    Raises:
        Exception: On Globus transfer or file access errors
    """
    if not GLOBUS_AVAILABLE:
        raise Exception(
            "globus-sdk is required for HuBMAP file transfers. "
            "Install it with: pip install globus-sdk"
        )

    if not DEST_ENDPOINT:
        raise Exception("GLOBUS_ENDPOINT_ID not configured")

    # Create temp file path in /tmp
    temp_filename = f"/tmp/globus_transfer_{uuid.uuid4()}_{filename}"

    try:
        # Initialize Globus TransferClient
        authorizer = AccessTokenAuthorizer(access_token)
        tc = TransferClient(authorizer=authorizer)

        logger.info(f"Initiating Globus transfer from {source_endpoint}{source_path} to {temp_filename}")

        # Submit transfer task
        tdata = TransferData(
            tc,
            source_endpoint,
            DEST_ENDPOINT,
            label=f"API transfer: {filename}"
        )
        tdata.add_item(source_path, temp_filename)
        task = tc.submit_transfer(tdata)
        task_id = task["task_id"]

        logger.info(f"Globus transfer task {task_id} submitted")

        # Stream file while transfer is in progress
        last_position = 0
        poll_interval = 0.5  # seconds

        while True:
            # Check transfer status
            task_status = tc.get_task(task_id)
            status = task_status["status"]

            logger.debug(f"Transfer task {task_id} status: {status}")

            # Read new bytes if file exists
            if os.path.exists(temp_filename):
                try:
                    with open(temp_filename, "rb") as f:
                        f.seek(last_position)
                        chunk = f.read(8192)
                        if chunk:
                            yield chunk
                            last_position = f.tell()
                            logger.debug(f"Streamed {len(chunk)} bytes")
                except OSError as e:
                    logger.error(f"Error reading temp file: {e}")
                    raise

            # Check if transfer is complete
            if status in ["SUCCEEDED", "FAILED"]:
                break

            # Wait before next poll
            await asyncio.sleep(poll_interval)

        # Handle transfer failure
        if status == "FAILED":
            error_msg = task_status.get("nice_status_details", "Unknown error")
            logger.error(f"Globus transfer failed: {error_msg}")
            raise Exception(f"Globus transfer failed: {error_msg}")

        logger.info(f"Globus transfer completed successfully")

        # Stream any remaining bytes
        if os.path.exists(temp_filename):
            try:
                with open(temp_filename, "rb") as f:
                    f.seek(last_position)
                    while chunk := f.read(8192):
                        yield chunk
                        logger.debug(f"Streamed final {len(chunk)} bytes")
            except OSError as e:
                logger.error(f"Error reading final bytes: {e}")
                raise

    except Exception as e:
        logger.error(f"Error during Globus transfer: {e}")
        raise

    finally:
        # Cleanup temp file
        if os.path.exists(temp_filename):
            try:
                os.remove(temp_filename)
                logger.info(f"Cleaned up temp file: {temp_filename}")
            except OSError as e:
                logger.warning(f"Failed to cleanup temp file {temp_filename}: {e}")
