"""REST API router for streaming files from DCCs."""

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import StreamingResponse, Response
from typing import Optional
import logging

from dcc2cvh.cvh import api
from dcc2cvh.cvh.services import drs, globus
from dcc2cvh.cvh.services.hubmap import fetch_access_metadata, extract_uuid_from_persistent_id
from dcc2cvh.cvh.models import FileMetadataModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])


@router.get("/{dcc}/{local_id}")
async def stream_file(
    dcc: str,
    local_id: str,
    authorization: Optional[str] = Header(None),
    range: Optional[str] = Header(None)
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

        # 6. Check HuBMAP access level and enforce access control
        if normalized_dcc == "hubmap":
            data_access_level = file_doc.get("data_access_level")

            # If access level not cached, fetch from Search API and cache it
            if data_access_level is None:
                logger.info(f"Access level not cached for {local_id}, querying HuBMAP Search API")

                # Extract UUID from persistent_id
                persistent_id = file_doc.get("persistent_id")
                if persistent_id:
                    uuid = extract_uuid_from_persistent_id(persistent_id)

                    if uuid:
                        # Fetch access metadata from Search API
                        metadata = await fetch_access_metadata(uuid)

                        if metadata and metadata.data_access_level:
                            # Cache in MongoDB for future requests
                            logger.debug(f"Caching access level '{metadata.data_access_level}' for {local_id}")

                            await api.db.file.update_one(
                                {"id_namespace": id_namespace, "local_id": local_id},
                                {"$set": {
                                    "status": metadata.status,
                                    "data_access_level": metadata.data_access_level
                                }}
                            )

                            data_access_level = metadata.data_access_level
                        else:
                            logger.warning(f"Could not fetch access level for {local_id} (UUID: {uuid})")
                    else:
                        logger.warning(f"Could not extract UUID from persistent_id: {persistent_id}")

            # If still unknown after fetch attempt, allow request to proceed
            # Let downstream Globus/DRS handle access control
            if data_access_level is None:
                logger.info(
                    f"HuBMAP file {local_id} has unknown access level. "
                    "Allowing request to proceed - downstream access control will enforce permissions."
                )
                # Don't block the request - continue to streaming logic

            # Enforce access control based on data_access_level
            elif data_access_level in ["consortium", "protected"]:
                if not auth_token:
                    if data_access_level == "consortium":
                        message = (
                            "This file requires consortium access. "
                            "You must be a member of the HuBMAP consortium to access this file. "
                            "Please authenticate with a Globus access token that has HuBMAP consortium membership. "
                            "Obtain a token from https://app.globus.org/ and include it in the "
                            "Authorization header as 'Bearer <token>'."
                        )
                    else:  # protected
                        message = (
                            "This file requires protected access. "
                            "This file contains protected data (genomic/HIPAA) and requires formal access approval. "
                            "You must: (1) be a HuBMAP consortium member, and (2) have completed data use agreements. "
                            "Please authenticate with a Globus access token and ensure you have the required access approvals. "
                            "Contact HuBMAP support at help@hubmapconsortium.org for access requests."
                        )

                    logger.info(f"Blocked {data_access_level} access to {local_id} - no authentication provided")
                    raise HTTPException(status_code=403, detail=message)

            # Public files - continue normally (no auth required)

        # 7. Determine access method (HTTPS or Globus)
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

                # Prepare response headers
                response_headers = {
                    "Content-Disposition": f'attachment; filename="{drs_object.name or "file"}"',
                    "Accept-Ranges": "bytes"
                }

                status_code = 200
                range_header_to_send = None

                # Handle Range request if present
                if range:
                    # Validate that file size is available
                    if not drs_object.size:
                        logger.warning(f"Range request for file without size metadata: {local_id}")
                        raise HTTPException(
                            status_code=500,
                            detail="Cannot process range request: file size unavailable"
                        )

                    try:
                        # Parse and validate the Range header
                        start, end, content_length = drs.parse_range_header(range, drs_object.size)

                        logger.debug(f"Range request: bytes {start}-{end}/{drs_object.size}")

                        # Set response for partial content
                        range_header_to_send = range
                        status_code = 206
                        response_headers["Content-Range"] = f"bytes {start}-{end}/{drs_object.size}"
                        response_headers["Content-Length"] = str(content_length)

                    except drs.RangeNotSatisfiableError as e:
                        # Range exceeds file bounds - return 416
                        logger.warning(f"Range not satisfiable: {range} for file size {e.file_size}")
                        raise HTTPException(
                            status_code=416,
                            headers={
                                "Content-Range": f"bytes */{e.file_size}",
                                "Accept-Ranges": "bytes"
                            }
                        )

                    except ValueError as e:
                        # Invalid Range header syntax - return 400
                        logger.warning(f"Invalid Range header syntax: {range}")
                        raise HTTPException(
                            status_code=400,
                            detail=f"Invalid Range header: {str(e)}"
                        )

                # Stream file (with or without range)
                chunk_gen = drs.stream_from_url(
                    download_url,
                    auth_headers,
                    range_header_to_send
                )

                # Set Content-Type from DRS metadata
                media_type = drs_object.mime_type or "application/octet-stream"

                # For full file requests, include Content-Length from DRS metadata if available
                if not range and drs_object.size:
                    response_headers["Content-Length"] = str(drs_object.size)

                return StreamingResponse(
                    chunk_gen,
                    status_code=status_code,
                    media_type=media_type,
                    headers=response_headers
                )

            except HTTPException:
                # Re-raise HTTP exceptions (400, 416, 500, etc.)
                raise
            except Exception as e:
                logger.error(f"HTTPS streaming error: {str(e)}")
                raise HTTPException(status_code=502, detail="Failed to stream file")

        elif has_globus:
            # Globus transfer + stream (HuBMAP path)

            # Reject Range requests for Globus transfers - not supported
            if range:
                logger.warning(f"Range request not supported for Globus transfer: {local_id}")
                raise HTTPException(
                    status_code=501,
                    detail="Range requests are not supported for this file. Globus transfers require full file download."
                )

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
