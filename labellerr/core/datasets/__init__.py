import json
import logging
import uuid
from typing import TYPE_CHECKING

from ... import schemas as root_schemas
from .. import constants, schemas
from ..connectors import create_connection
from ..exceptions import LabellerrError
from .audio_dataset import AudioDataSet as LabellerrAudioDataset
from .base import LabellerrDataset
from .document_dataset import DocumentDataSet as LabellerrDocumentDataset
from .image_dataset import ImageDataset as LabellerrImageDataset
from .utils import upload_files, upload_folder_files_to_dataset
from .video_dataset import VideoDataset as LabellerrVideoDataset

if TYPE_CHECKING:
    from ..client import LabellerrClient

__all__ = [
    "LabellerrImageDataset",
    "LabellerrVideoDataset",
    "LabellerrDataset",
    "LabellerrAudioDataset",
    "LabellerrDocumentDataset",
]


def create_dataset(
    client: "LabellerrClient",
    dataset_config: schemas.DatasetConfig,
    files_to_upload=None,
    folder_to_upload=None,
    path=None,
    connection_id=None,
    connector_config=None,
):
    """
    Creates a dataset with support for multiple data types and connectors.

    :param dataset_config: A dictionary containing the configuration for the dataset.
                          Required fields: client_id, dataset_name, data_type
                          Optional fields: dataset_description, connector_type
                          Can also be a DatasetConfig Pydantic model instance.
    :param files_to_upload: List of file paths to upload (for local connector)
    :param folder_to_upload: Path to folder to upload (for local connector)
    :param connection_id: Pre-existing connection ID to use for the dataset.
                         Either connection_id or connector_config can be provided, but not both.
    :param connector_config: Configuration for cloud connectors (GCP/AWS)
                            Can be a dict or AWSConnectorConfig/GCPConnectorConfig model instance.
                            Either connection_id or connector_config can be provided, but not both.
    :return: A dictionary containing the response status and the ID of the created dataset.
    :raises LabellerrError: If both connection_id and connector_config are provided.
    """

    try:
        # Validate that both connection_id and connector_config are not provided
        if connection_id is not None and connector_config is not None:
            raise LabellerrError(
                "Cannot provide both connection_id and connector_config. "
                "Use connection_id for existing connections or connector_config to create a new connection."
            )

        connector_type = dataset_config.connector_type
        # Use provided connection_id or set to None (will be created later if needed)
        final_connection_id = connection_id

        # Handle different connector types only if connection_id is not provided
        if final_connection_id is None:
            if connector_type == "local":
                if files_to_upload is not None:
                    try:
                        final_connection_id = upload_files(
                            client,
                            client_id=client.client_id,
                            files_list=files_to_upload,
                        )
                    except Exception as e:
                        raise LabellerrError(
                            f"Failed to upload files to dataset: {str(e)}"
                        )

                elif folder_to_upload is not None:
                    try:
                        result = upload_folder_files_to_dataset(
                            client,
                            {
                                "client_id": client.client_id,
                                "folder_path": folder_to_upload,
                                "data_type": dataset_config.data_type,
                            },
                        )
                        final_connection_id = result["connection_id"]
                    except Exception as e:
                        raise LabellerrError(
                            f"Failed to upload folder files to dataset: {str(e)}"
                        )
                elif connector_config is None:
                    # Create empty dataset for local connector
                    final_connection_id = None

            elif connector_type in ["gcp", "aws"]:
                if connector_config is None:
                    raise LabellerrError(
                        f"connector_config is required for {connector_type} connector when connection_id is not provided"
                    )
                if path is None:
                    raise LabellerrError(
                        f"path is required for {connector_type} connector"
                    )

                # Validate connector_config using Pydantic models
                if connector_type == "aws":
                    if not isinstance(
                        connector_config, root_schemas.AWSConnectorConfig
                    ):
                        validated_connector = root_schemas.AWSConnectorConfig(
                            **connector_config
                        )
                    else:
                        validated_connector = connector_config
                else:  # gcp
                    if not isinstance(
                        connector_config, root_schemas.GCPConnectorConfig
                    ):
                        validated_connector = root_schemas.GCPConnectorConfig(
                            **connector_config
                        )
                    else:
                        validated_connector = connector_config

                try:
                    final_connection_id = create_connection(
                        client,
                        connector_type,
                        client.client_id,
                        validated_connector.model_dump(),
                    )
                except Exception as e:
                    raise LabellerrError(
                        f"Failed to setup {connector_type} connector: {str(e)}"
                    )
            else:
                raise LabellerrError(f"Unsupported connector type: {connector_type}")

        unique_id = str(uuid.uuid4())
        url = f"{constants.BASE_URL}/datasets/create?client_id={client.client_id}&uuid={unique_id}"

        payload = json.dumps(
            {
                "dataset_name": dataset_config.dataset_name,
                "dataset_description": dataset_config.dataset_description,
                "data_type": dataset_config.data_type,
                "connection_id": final_connection_id,
                "path": path,
                "client_id": client.client_id,
                "connector_type": connector_type,
            }
        )
        response_data = client.make_request(
            "POST",
            url,
            extra_headers={"content-type": "application/json"},
            request_id=unique_id,
            data=payload,
        )
        dataset_id = response_data["response"]["dataset_id"]
        return LabellerrDataset(client=client, dataset_id=dataset_id)  # type: ignore[abstract]

    except LabellerrError as e:
        logging.error(f"Failed to create dataset: {e}")
        raise
