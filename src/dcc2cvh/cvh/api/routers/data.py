"""REST API router for streaming files from DCCs."""

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional
import logging

from dcc2cvh.cvh import api
from dcc2cvh.cvh.services import drs, globus
from dcc2cvh.cvh.models import FileMetadataModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])


@router.get("/{dcc}/{local_id}")
async def stream_file(
    dcc: str,
    local_id: str,
    authorization: Optional[str] = Header(None)
):
    """
    Stream file from DCC via HTTPS or Globus using file metadata from database.

    For 4DN files: Streams file contents directly via HTTPS.
    For HuBMAP files: Initiates Globus transfer and streams during download.

    Path Parameters:
        dcc: DCC abbreviation (4dn, hubmap) - case insensitive
        local_id: The file's unique ID within the DCC

    Headers:
        Authorization: Optional "Bearer <token>" for authentication
            - Required for HuBMAP files (Globus access token)
            - Optional for 4DN files

    Returns:
        StreamingResponse with file contents streamed as binary data

    Raises:
        404: File not found in database
        401: Authentication required but not provided
        501: No supported access method available
        502: Upstream service error
        504: Service timeout
    """
    try:
        # 1. Validate and normalize DCC name
        from dcc2cvh.dcc_registry import normalize_dcc_name, get_all_dcc_names

        normalized_dcc = normalize_dcc_name(dcc)
        valid_dccs = get_all_dcc_names()

        if normalized_dcc not in valid_dccs:
            logger.warning(f"Invalid DCC requested: {dcc}")
            raise HTTPException(
                status_code=400,
                detail=f"Unknown DCC '{dcc}'. Valid DCCs: {', '.join(valid_dccs)}"
            )

        # 2. Look up DCC metadata to get id_namespace
        if api.db is None:
            logger.error("Database not initialized")
            raise HTTPException(status_code=500, detail="Database not available")

        # Look up DCC by normalized abbreviation (case-insensitive)
        dcc_doc = await api.db.dcc.find_one({
            "dcc_abbreviation": {"$regex": f"^{normalized_dcc}", "$options": "i"}
        })

        if not dcc_doc:
            logger.warning(f"DCC metadata not found: {dcc}")
            raise HTTPException(status_code=500, detail=f"DCC configuration not found: {dcc}")

        id_namespace = dcc_doc.get("project_id_namespace")

        if not id_namespace:
            logger.error(f"DCC missing project_id_namespace: {dcc}")
            raise HTTPException(status_code=500, detail=f"DCC configuration incomplete: {dcc}")

        # 3. Look up file in MongoDB by composite key
        logger.info(f"Looking up file: id_namespace={id_namespace}, local_id={local_id}")

        file_doc = await api.db.file.find_one({
            "id_namespace": id_namespace,
            "local_id": local_id
        })

        if not file_doc:
            logger.warning(f"File not found: {id_namespace}/{local_id}")
            raise HTTPException(status_code=404, detail="File not found")

        # 2. Parse file metadata
        try:
            # Extract only the fields needed for the API from the database document
            file_data = {
                k: v for k, v in file_doc.items()
                if k in FileMetadataModel.__fields__
                and k not in ('dcc', 'collections')  # Skip required fields not in database
            }
            file_metadata = FileMetadataModel(**file_data)
        except Exception as e:
            logger.error(f"Failed to parse file metadata: {str(e)}")
            # Try to extract just the access_url if full parsing fails
            access_url = file_doc.get("access_url")
            if not access_url:
                raise HTTPException(status_code=500, detail="Invalid file metadata in database")
            # Create a minimal metadata object with just access_url
            from dataclasses import dataclass
            @dataclass
            class MinimalMetadata:
                access_url: str
            file_metadata = MinimalMetadata(access_url=access_url)
            file_metadata.filename = file_doc.get("filename", "file")

        # 3. Extract auth token from Authorization header
        auth_token = None
        if authorization:
            if authorization.startswith("Bearer "):
                auth_token = authorization[7:]  # Remove "Bearer " prefix
            else:
                auth_token = authorization

        # 4. Check if file has access_url
        if not file_metadata.access_url:
            logger.warning(f"File has no access_url: {id_namespace}/{local_id}")
            raise HTTPException(status_code=501, detail="File has no access URL")

        logger.info(f"File access_url: {file_metadata.access_url}")

        # 5. Fetch DRS object metadata
        try:
            drs_object = await drs.fetch_drs_object(file_metadata.access_url, auth_token)
        except ValueError as e:
            logger.warning(f"Invalid DRS URI: {file_metadata.access_url}")
            raise HTTPException(status_code=400, detail=f"Invalid file access URL: {str(e)}")
        except Exception as e:
            logger.error(f"DRS metadata fetch failed: {str(e)}")
            if "not found" in str(e).lower():
                raise HTTPException(status_code=404, detail="File not found in repository")
            elif "authentication" in str(e).lower() or "forbidden" in str(e).lower():
                raise HTTPException(status_code=401, detail="Authentication required")
            elif "timeout" in str(e).lower():
                raise HTTPException(status_code=504, detail="Repository service timeout")
            else:
                raise HTTPException(status_code=502, detail="Failed to fetch file metadata")

        # 5. Determine access method (HTTPS or Globus)
        has_globus = any(m.type == "globus" for m in drs_object.access_methods)
        has_https = any(m.type in ["https", "s3"] for m in drs_object.access_methods)

        logger.debug(f"Access methods available: HTTPS={has_https}, Globus={has_globus}")

        # 6. Stream file using appropriate method
        if has_https:
            # Direct HTTPS streaming (4DN path)
            try:
                download_url = await drs.get_https_download_url(drs_object.access_methods)
                logger.info(f"Streaming HTTPS file: {drs_object.name}")

                auth_headers = None
                if authorization:
                    auth_headers = {"Authorization": authorization}

                return StreamingResponse(
                    drs.stream_from_url(download_url, auth_headers),
                    media_type=drs_object.mime_type or "application/octet-stream",
                    headers={
                        "Content-Disposition": f'attachment; filename="{drs_object.name or "file"}"'
                    }
                )

            except Exception as e:
                logger.error(f"HTTPS streaming error: {str(e)}")
                raise HTTPException(status_code=502, detail="Failed to stream file")

        elif has_globus:
            # Globus transfer + stream (HuBMAP path)
            if not auth_token:
                logger.warning(f"HuBMAP file requested without Globus token")
                raise HTTPException(
                    status_code=401,
                    detail="Globus access token required for this file"
                )

            try:
                globus_info = await drs.extract_globus_info(drs_object.access_methods)
                logger.info(
                    f"Initiating Globus transfer: {drs_object.name} "
                    f"from {globus_info['endpoint_id']}{globus_info['path']}"
                )

                return StreamingResponse(
                    globus.transfer_and_stream(
                        globus_info["endpoint_id"],
                        globus_info["path"],
                        drs_object.name or "file",
                        auth_token
                    ),
                    media_type=drs_object.mime_type or "application/octet-stream",
                    headers={
                        "Content-Disposition": f'attachment; filename="{drs_object.name or "file"}"'
                    }
                )

            except ValueError as e:
                logger.error(f"Invalid Globus access method: {str(e)}")
                raise HTTPException(status_code=502, detail="Invalid Globus metadata")
            except Exception as e:
                logger.error(f"Globus transfer error: {str(e)}")
                if "not configured" in str(e).lower():
                    raise HTTPException(
                        status_code=501,
                        detail="Globus transfer not configured on server"
                    )
                else:
                    raise HTTPException(status_code=502, detail="Globus transfer failed")

        else:
            logger.warning(f"No supported access method for {dcc}/{local_id}")
            raise HTTPException(
                status_code=501,
                detail="No supported access method available for this file"
            )

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"Unexpected error in stream_file: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
