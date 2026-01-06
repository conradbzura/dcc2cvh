"""REST API router for sync operations."""

import logging
import os
import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel

from cfdb.services.sync import is_sync_running, start_sync

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sync", tags=["sync"])

SYNC_API_KEY = os.getenv("SYNC_API_KEY", "")


async def verify_api_key(x_api_key: str = Header(..., alias="X-API-Key")):
    """Verify API key for sync endpoints."""
    if not SYNC_API_KEY:
        raise HTTPException(
            status_code=500, detail="SYNC_API_KEY not configured on server"
        )
    if x_api_key != SYNC_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key


class SyncResponse(BaseModel):
    task_id: str
    status: str
    dcc_names: list[str]
    message: str


@router.post("", response_model=SyncResponse, status_code=202)
async def sync(
    dccs: list[str] = Query(default=[]),
    _: str = Depends(verify_api_key),
):
    """
    Start a new sync task.

    The sync runs as a background task. Returns immediately with a task ID.
    Only one sync can run at a time - returns 409 if a sync is already in progress.

    Query Parameters:
        dccs: List of DCC names to sync (e.g., ?dccs=4dn&dccs=hubmap).
              If empty, all DCCs will be synced.

    Returns:
        202 Accepted with task details

    Raises:
        401: Invalid API key
        409: Sync already in progress
        500: Server configuration error
    """
    if is_sync_running():
        raise HTTPException(
            status_code=409,
            detail="A sync task is already running. Please wait for it to complete.",
        )

    task_id = str(uuid.uuid4())

    try:
        task = await start_sync(task_id, dccs)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        # Race condition - another sync started between check and start
        raise HTTPException(status_code=409, detail=str(e))

    logger.info(f"Started sync task {task_id} for DCCs: {task.dcc_names}")

    return SyncResponse(
        task_id=task.id,
        status=task.status.value,
        dcc_names=task.dcc_names,
        message=f"Sync started for {', '.join(task.dcc_names)}",
    )
