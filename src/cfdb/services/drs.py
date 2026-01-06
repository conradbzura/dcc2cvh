"""Data Repository Service (DRS) integration for file streaming."""

import asyncio
import logging
import urllib.parse
from typing import AsyncGenerator, List, Optional

import aiohttp
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class RangeNotSatisfiableError(Exception):
    """Raised when requested byte range cannot be satisfied."""

    def __init__(self, file_size: int):
        self.file_size = file_size
        super().__init__(f"Range not satisfiable for file of size {file_size}")


class DRSAccessMethod(BaseModel):
    """GA4GH DRS access method for retrieving object bytes."""

    type: str  # e.g., "https", "s3", "globus", "gs"
    access_url: Optional[str] = None  # Changed from HttpUrl to str for compatibility
    access_id: Optional[str] = None
    region: Optional[str] = None
    headers: Optional[dict] = None


class DRSObject(BaseModel):
    """GA4GH DRS object with metadata and access methods."""

    id: str
    name: Optional[str] = None
    size: Optional[int] = None
    checksums: Optional[List[dict]] = None
    access_methods: List[DRSAccessMethod]
    mime_type: Optional[str] = None


async def parse_drs_uri(drs_uri: str) -> tuple:
    """
    Parse DRS URI into hostname and object ID.

    Args:
        drs_uri: DRS URI (e.g., drs://drs.hubmapconsortium.org/abc123)

    Returns:
        Tuple of (hostname, object_id)

    Raises:
        ValueError: If URI is invalid or not a DRS URI
    """
    parsed = urllib.parse.urlparse(drs_uri)

    if parsed.scheme != "drs":
        raise ValueError(
            f"Invalid DRS URI: must start with drs://, got {parsed.scheme}://"
        )

    hostname = parsed.netloc
    object_id = parsed.path.lstrip("/")

    if not hostname or not object_id:
        raise ValueError(f"Invalid DRS URI format: {drs_uri}")

    return hostname, object_id


def parse_range_header(range_header: str, file_size: int) -> tuple[int, int, int]:
    """
    Parse and validate HTTP Range header against file size.

    Supports formats:
    - bytes=start-end (specific range)
    - bytes=start- (from start to end of file)
    - bytes=-suffix (last N bytes)

    Args:
        range_header: Range header value (e.g., "bytes=0-1023")
        file_size: Total file size in bytes

    Returns:
        Tuple of (start_byte, end_byte, content_length)

    Raises:
        ValueError: If range syntax is invalid
        RangeNotSatisfiableError: If range exceeds file bounds
    """
    # Validate format: must start with "bytes="
    if not range_header.startswith("bytes="):
        raise ValueError("Range header must start with 'bytes='")

    # Extract range part after "bytes="
    range_spec = range_header[6:].strip()

    # Reject multipart range requests (multiple ranges)
    if "," in range_spec:
        raise ValueError("Multipart range requests are not supported")

    # Parse start and end
    if "-" not in range_spec:
        raise ValueError("Invalid range format: missing '-'")

    parts = range_spec.split("-", 1)
    start_str, end_str = parts[0].strip(), parts[1].strip()

    # Handle suffix-length format: bytes=-500 (last 500 bytes)
    if not start_str and end_str:
        try:
            suffix_length = int(end_str)
        except ValueError:
            raise ValueError("Suffix length must be an integer")
        if suffix_length <= 0:
            raise ValueError("Suffix length must be positive")
        start = max(0, file_size - suffix_length)
        end = file_size - 1

    # Handle open-ended format: bytes=1000- (from byte 1000 to end)
    elif start_str and not end_str:
        try:
            start = int(start_str)
        except ValueError:
            raise ValueError("Start byte must be an integer")
        end = file_size - 1

    # Handle specific range: bytes=0-1023
    elif start_str and end_str:
        try:
            start = int(start_str)
            end = int(end_str)
        except ValueError:
            raise ValueError("Start and end bytes must be integers")

    else:
        raise ValueError("Invalid range format")

    # Validate bounds
    if start < 0 or end < 0:
        raise ValueError("Range values cannot be negative")

    if start > end:
        raise ValueError("Start byte must be <= end byte")

    if start >= file_size:
        raise RangeNotSatisfiableError(file_size)

    # Clamp end to file size
    end = min(end, file_size - 1)

    content_length = end - start + 1

    return start, end, content_length


async def fetch_drs_object(drs_uri: str, auth_token: Optional[str] = None) -> DRSObject:
    """
    Fetch DRS object metadata from GA4GH DRS API.

    Args:
        drs_uri: DRS URI (e.g., drs://drs.hubmapconsortium.org/abc123)
        auth_token: Optional Bearer token for authentication

    Returns:
        DRSObject with metadata and access methods

    Raises:
        ValueError: If DRS URI is invalid
        aiohttp.ClientError: On network errors
        Exception: On DRS API errors or timeouts
    """
    hostname, object_id = await parse_drs_uri(drs_uri)

    # Construct GA4GH DRS API endpoint
    drs_api_url = f"https://{hostname}/ga4gh/drs/v1/objects/{object_id}"

    logger.debug(f"Fetching DRS metadata from {drs_api_url}")

    async with aiohttp.ClientSession() as session:
        headers = {}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        try:
            async with session.get(
                drs_api_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Retrieved DRS object {object_id}")

                    # Parse access methods
                    access_methods = []
                    for method_data in data.get("access_methods", []):
                        # Normalize access_url - it can be a string or a dict with "url" key
                        method_copy = method_data.copy()
                        if isinstance(method_copy.get("access_url"), dict):
                            method_copy["access_url"] = method_copy["access_url"].get(
                                "url"
                            )
                        access_methods.append(DRSAccessMethod(**method_copy))

                    return DRSObject(
                        id=data.get("id", object_id),
                        name=data.get("name"),
                        size=data.get("size"),
                        checksums=data.get("checksums"),
                        access_methods=access_methods,
                        mime_type=data.get("mime_type"),
                    )

                elif response.status == 404:
                    raise Exception(f"DRS object not found: {object_id}")

                elif response.status == 401:
                    raise Exception("Authentication required for this DRS object")

                elif response.status == 403:
                    raise Exception("Access forbidden for this DRS object")

                else:
                    raise Exception(f"DRS API error: HTTP {response.status}")

        except asyncio.TimeoutError:
            raise Exception(f"DRS service timeout for {object_id}")
        except aiohttp.ClientError as e:
            raise Exception(f"Network error fetching DRS metadata: {e}")


async def get_https_download_url(access_methods: List[DRSAccessMethod]) -> str:
    """
    Extract HTTPS download URL from access methods.

    Args:
        access_methods: List of DRS access methods

    Returns:
        Download URL string

    Raises:
        ValueError: If no HTTPS/S3 access method found
    """
    for method in access_methods:
        if method.type in ["https", "s3"]:
            if not method.access_url:
                continue
            return str(method.access_url)

    raise ValueError("No HTTPS or S3 access method found")


async def stream_from_url(
    url: str, range_header: Optional[str] = None
) -> AsyncGenerator[bytes, None]:
    """
    Stream file bytes from HTTPS URL with optional Range request support.

    Args:
        url: HTTPS download URL
        auth_headers: Optional authentication headers
        range_header: Optional Range header value to forward to upstream

    Yields:
        Chunks of file bytes

    Raises:
        Exception: On download errors
    """
    headers = {}

    # Add Range header if provided
    if range_header:
        headers["Range"] = range_header

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=None, connect=30),
            ) as response:
                # Accept 200 (full file) or 206 (partial content)
                if response.status not in (200, 206):
                    raise Exception(f"Failed to download file: HTTP {response.status}")

                # Stream chunks while response context is open
                async for chunk in response.content.iter_chunked(8192):
                    if chunk:
                        yield chunk

        except asyncio.TimeoutError:
            raise Exception(f"Timeout downloading file from {url}")
        except aiohttp.ClientError as e:
            raise Exception(f"Network error downloading file: {e}")
