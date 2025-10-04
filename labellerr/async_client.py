# labellerr/async_client.py

import asyncio
import logging
import os
import uuid
from typing import Any, Dict, List, Optional

import aiofiles
import aiohttp

from . import constants, client_utils
from .exceptions import LabellerrError


class AsyncLabellerrClient:
    """
    Async client for interacting with the Labellerr API using aiohttp for better performance.
    """

    def __init__(self, api_key: str, api_secret: str, connector_limit: int = 100):
        """
        Initializes the AsyncLabellerrClient with API credentials.

        :param api_key: The API key for authentication.
        :param api_secret: The API secret for authentication.
        :param connector_limit: Maximum number of connections in the pool
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = constants.BASE_URL
        self._session: Optional[aiohttp.ClientSession] = None
        self._connector_limit = connector_limit

    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def _ensure_session(self):
        """Ensure aiohttp session is created with connection pooling."""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=self._connector_limit,
                limit_per_host=20,
                keepalive_timeout=30,
                enable_cleanup_closed=True,
            )
            timeout = aiohttp.ClientTimeout(total=300, connect=30)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={"User-Agent": "Labellerr-SDK-Async/1.0"},
            )

    async def close(self):
        """Close the aiohttp session and cleanup resources."""
        if self._session and not self._session.closed:
            await self._session.close()

    def _build_headers(
        self,
        client_id: Optional[str] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        """
        Builds standard headers for API requests.

        :param client_id: Optional client ID to include in headers
        :param extra_headers: Optional dictionary of additional headers
        :return: Dictionary of headers
        """
        return client_utils.build_headers(
            api_key=self.api_key,
            api_secret=self.api_secret,
            source="sdk-async",
            client_id=client_id,
            extra_headers=extra_headers,
        )

    async def _handle_response(
        self, response: aiohttp.ClientResponse, request_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Async standardized response handling.

        :param response: aiohttp ClientResponse object
        :param request_id: Optional request tracking ID
        :return: JSON response data for successful requests
        :raises LabellerrError: For non-successful responses
        """
        if response.status in [200, 201]:
            try:
                return await response.json()
            except Exception:
                text = await response.text()
                raise LabellerrError(f"Expected JSON response but got: {text}")
        elif 400 <= response.status < 500:
            try:
                error_data = await response.json()
                raise LabellerrError({"error": error_data, "code": response.status})
            except Exception:
                text = await response.text()
                raise LabellerrError({"error": text, "code": response.status})
        else:
            text = await response.text()
            raise LabellerrError(
                {
                    "status": "internal server error",
                    "message": "Please contact support with the request tracking id",
                    "request_id": request_id or str(uuid.uuid4()),
                    "error": text,
                }
            )

    async def get_direct_upload_url(
        self, file_name: str, client_id: str, purpose: str = "pre-annotations"
    ) -> str:
        """
        Async version of get_direct_upload_url.
        """
        await self._ensure_session()

        url = f"{constants.BASE_URL}/connectors/direct-upload-url"
        params = {"client_id": client_id, "purpose": purpose, "file_name": file_name}
        headers = self._build_headers(client_id=client_id)

        try:
            async with self._session.get(
                url, params=params, headers=headers
            ) as response:
                response_data = await self._handle_response(response)
                return response_data["response"]
        except Exception as e:
            logging.exception(f"Error getting direct upload url: {e}")
            raise

    async def connect_local_files(
        self, client_id: str, file_names: List[str], connection_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Async version of connect_local_files.
        """
        await self._ensure_session()

        url = f"{constants.BASE_URL}/connectors/connect/local"
        params = {"client_id": client_id}
        headers = self._build_headers(client_id=client_id)

        body = {"file_names": file_names}
        if connection_id is not None:
            body["temporary_connection_id"] = connection_id

        async with self._session.post(
            url, params=params, headers=headers, json=body
        ) as response:
            return await self._handle_response(response)

    async def upload_file_stream(
        self, signed_url: str, file_path: str, chunk_size: int = 8192
    ) -> bool:
        """
        Async streaming file upload to minimize memory usage.

        :param signed_url: GCS signed URL for upload
        :param file_path: Local file path to upload
        :param chunk_size: Size of chunks to read
        :return: True on success
        """
        await self._ensure_session()

        file_size = os.path.getsize(file_path)
        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(file_size),
        }

        async with aiofiles.open(file_path, "rb") as f:
            async with self._session.put(
                signed_url, headers=headers, data=f
            ) as response:
                if response.status not in [200, 201]:
                    text = await response.text()
                    raise LabellerrError(f"Upload failed: {response.status} - {text}")
                return True

    async def upload_files_batch(
        self, client_id: str, files_list: List[str], batch_size: int = 5
    ) -> str:
        """
        Async batch file upload with concurrency control.

        :param client_id: The ID of the client
        :param files_list: List of file paths to upload
        :param batch_size: Number of concurrent uploads
        :return: Connection ID
        """
        if isinstance(files_list, str):
            files_list = files_list.split(",")
        elif not isinstance(files_list, list):
            raise LabellerrError(
                "files_list must be either a list or a comma-separated string"
            )

        if len(files_list) == 0:
            raise LabellerrError("No files to upload")

        # Validate files exist
        for file_path in files_list:
            if not os.path.exists(file_path):
                raise LabellerrError(f"File does not exist: {file_path}")
            if not os.path.isfile(file_path):
                raise LabellerrError(f"Path is not a file: {file_path}")

        # Get upload URLs and connection ID
        file_names = [os.path.basename(f) for f in files_list]
        response = await self.connect_local_files(client_id, file_names)

        connection_id = response["response"]["temporary_connection_id"]
        resumable_upload_links = response["response"]["resumable_upload_links"]

        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(batch_size)

        async def upload_single_file(file_path: str):
            async with semaphore:
                file_name = os.path.basename(file_path)
                signed_url = resumable_upload_links[file_name]
                return await self.upload_file_stream(signed_url, file_path)

        # Upload files concurrently
        tasks = [upload_single_file(file_path) for file_path in files_list]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check for errors
        failed_files = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed_files.append((files_list[i], str(result)))

        if failed_files:
            error_msg = (
                f"Failed to upload {len(failed_files)} files: {failed_files[:3]}"
            )
            if len(failed_files) > 3:
                error_msg += f"... and {len(failed_files) - 3} more"
            raise LabellerrError(error_msg)

        return connection_id

    async def get_dataset(self, workspace_id: str, dataset_id: str) -> Dict[str, Any]:
        """
        Async version of get_dataset.
        """
        await self._ensure_session()

        url = f"{constants.BASE_URL}/datasets/{dataset_id}"
        params = {"client_id": workspace_id, "uuid": str(uuid.uuid4())}
        headers = self._build_headers(
            extra_headers={"Origin": constants.ALLOWED_ORIGINS}
        )

        async with self._session.get(url, params=params, headers=headers) as response:
            return await self._handle_response(response)

    async def create_dataset(
        self,
        dataset_config: Dict[str, Any],
        files_to_upload: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Async version of create_dataset.
        """
        await self._ensure_session()

        try:
            # Validate data_type
            if dataset_config.get("data_type") not in constants.DATA_TYPES:
                raise LabellerrError(
                    f"Invalid data_type. Must be one of {constants.DATA_TYPES}"
                )

            connection_id = None
            if files_to_upload is not None:
                connection_id = await self.upload_files_batch(
                    client_id=dataset_config["client_id"], files_list=files_to_upload
                )

            unique_id = str(uuid.uuid4())
            url = f"{constants.BASE_URL}/datasets/create"
            params = {"client_id": dataset_config["client_id"], "uuid": unique_id}
            headers = self._build_headers(
                client_id=dataset_config["client_id"],
                extra_headers={"content-type": "application/json"},
            )

            payload = {
                "dataset_name": dataset_config["dataset_name"],
                "dataset_description": dataset_config.get("dataset_description", ""),
                "data_type": dataset_config["data_type"],
                "connection_id": connection_id,
                "path": "local",
                "client_id": dataset_config["client_id"],
            }

            async with self._session.post(
                url, params=params, headers=headers, json=payload
            ) as response:
                response_data = await self._handle_response(response, unique_id)
                dataset_id = response_data["response"]["dataset_id"]
                return {"response": "success", "dataset_id": dataset_id}

        except LabellerrError as e:
            logging.error(f"Failed to create dataset: {e}")
            raise

    # Add more async methods as needed...
