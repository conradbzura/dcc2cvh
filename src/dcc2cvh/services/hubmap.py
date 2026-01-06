"""HuBMAP Search API integration for access level metadata."""

import asyncio
import re
from typing import Optional
from pydantic import BaseModel
import aiohttp
import logging

logger = logging.getLogger(__name__)


class HuBMAPSearchResult(BaseModel):
    """HuBMAP Search API entity result."""
    uuid: str
    status: Optional[str] = None
    data_access_level: Optional[str] = None
    entity_type: Optional[str] = None


def extract_uuid_from_persistent_id(persistent_id: str) -> Optional[str]:
    """
    Extract UUID from HuBMAP persistent ID.

    Handles formats:
    - doi:10.35079/HBM123.ABCD.456
    - HBM123.ABCD.456
    - Direct UUID format (8-4-4-4-12)

    Returns UUID string or None if extraction fails.
    """
    if not persistent_id:
        return None

    # Try direct UUID pattern match
    uuid_pattern = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
    match = re.search(uuid_pattern, persistent_id, re.IGNORECASE)
    if match:
        return match.group(0).lower()

    return None


async def fetch_access_metadata(uuid: str) -> Optional[HuBMAPSearchResult]:
    """
    Fetch access level metadata from HuBMAP Search API.

    Args:
        uuid: HuBMAP entity UUID

    Returns:
        HuBMAPSearchResult or None if fetch fails (graceful degradation)
    """
    search_url = f"https://search.api.hubmapconsortium.org/v3/entities/{uuid}"

    logger.debug(f"Fetching HuBMAP access metadata: {uuid}")

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                search_url,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return HuBMAPSearchResult(
                        uuid=data.get("uuid", uuid),
                        status=data.get("status"),
                        data_access_level=data.get("data_access_level"),
                        entity_type=data.get("entity_type")
                    )
                elif response.status == 404:
                    logger.debug(f"HuBMAP entity not found: {uuid}")
                    return None
                else:
                    logger.warning(f"HuBMAP Search API error: HTTP {response.status}")
                    return None

        except asyncio.TimeoutError:
            logger.debug(f"Timeout fetching HuBMAP metadata: {uuid}")
            return None
        except aiohttp.ClientError as e:
            logger.debug(f"Network error fetching HuBMAP metadata: {e}")
            return None
        except Exception as e:
            logger.warning(f"Unexpected error fetching HuBMAP metadata: {e}")
            return None
