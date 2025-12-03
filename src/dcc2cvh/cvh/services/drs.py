"""Data Repository Service (DRS) integration for file streaming."""

import asyncio
import urllib.parse
from typing import Optional, AsyncGenerator, List
from pydantic import BaseModel, HttpUrl
import aiohttp
import logging

logger = logging.getLogger(__name__)


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
        raise ValueError(f"Invalid DRS URI: must start with drs://, got {parsed.scheme}://")

    hostname = parsed.netloc
    object_id = parsed.path.lstrip("/")

    if not hostname or not object_id:
        raise ValueError(f"Invalid DRS URI format: {drs_uri}")

    return hostname, object_id


async def fetch_drs_object(
    drs_uri: str,
    auth_token: Optional[str] = None
) -> DRSObject:
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
                            method_copy["access_url"] = method_copy["access_url"].get("url")
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


async def extract_globus_info(access_methods: List[DRSAccessMethod]) -> dict:
    """
    Extract Globus endpoint and path from access methods.

    Args:
        access_methods: List of DRS access methods

    Returns:
        Dict with "endpoint_id" and "path" keys

    Raises:
        ValueError: If no Globus access method found
    """
    for method in access_methods:
        if method.type == "globus":
            # Globus access_url format: globus://endpoint-uuid/path/to/file
            if not method.access_url:
                raise ValueError("Globus access method missing access_url")

            parsed = urllib.parse.urlparse(str(method.access_url))
            if parsed.scheme != "globus":
                raise ValueError(f"Invalid Globus URL format: {method.access_url}")

            endpoint_id = parsed.netloc
            path = parsed.path

            if not endpoint_id or not path:
                raise ValueError(f"Invalid Globus URL format: {method.access_url}")

            return {"endpoint_id": endpoint_id, "path": path}

    raise ValueError("No Globus access method found")


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
    url: str,
    auth_headers: Optional[dict] = None
) -> AsyncGenerator[bytes, None]:
    """
    Stream file bytes from HTTPS URL.

    Args:
        url: Download URL
        auth_headers: Optional headers to include in request

    Yields:
        Chunks of file bytes

    Raises:
        Exception: On download errors
    """
    headers = auth_headers or {}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=None, connect=30)
            ) as response:
                if response.status != 200:
                    raise Exception(f"Failed to download file: HTTP {response.status}")

                async for chunk in response.content.iter_chunked(8192):
                    if chunk:
                        yield chunk

        except asyncio.TimeoutError:
            raise Exception("Download timeout")
        except aiohttp.ClientError as e:
            raise Exception(f"Download error: {e}")
