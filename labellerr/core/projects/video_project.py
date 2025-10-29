import uuid
from typing import List

from .. import constants
from ..exceptions import LabellerrError
from ..schemas import DatasetDataType, KeyFrame
from ..utils import validate_params
from .base import LabellerrProject, LabellerrProjectMeta


class VideoProject(LabellerrProject):
    """
    Class for handling video project operations and fetching multiple datasets.
    """

    @validate_params(file_id=str, keyframes=list)
    def add_or_update_keyframes(
        self,
        file_id: str,
        keyframes: List[KeyFrame],
    ):
        """
        Links key frames to a file in a project.

        :param file_id: The ID of the file
        :param keyframes: List of KeyFrame objects to link
        :return: Response from the API
        """
        try:
            unique_id = str(uuid.uuid4())
            url = f"{constants.BASE_URL}/actions/add_update_keyframes?client_id={self.client.client_id}&uuid={unique_id}"

            body = {
                "project_id": self.project_id,
                "file_id": file_id,
                "keyframes": [
                    (kf.model_dump() if hasattr(kf, "model_dump") else kf)
                    for kf in keyframes
                ],
            }

            return self.client.make_request(
                "POST",
                url,
                extra_headers={"content-type": "application/json"},
                request_id=unique_id,
                json=body,
            )

        except LabellerrError as e:
            raise e
        except Exception as e:
            raise LabellerrError(f"Failed to link key frames: {str(e)}")

    @validate_params(file_id=str, keyframes=list)
    def delete_keyframes(self, file_id: str, keyframes: List[int]):
        """
        Deletes key frames from a project.

        :param file_id: The ID of the file
        :param keyframes: List of key frame numbers to delete
        :return: Response from the API
        """
        try:
            unique_id = str(uuid.uuid4())
            url = f"{constants.BASE_URL}/actions/delete_keyframes?project_id={self.project_id}&uuid={unique_id}&client_id={self.client.client_id}"

            return self.client.make_request(
                "POST",
                url,
                extra_headers={"content-type": "application/json"},
                request_id=unique_id,
                json={
                    "project_id": self.project_id,
                    "file_id": file_id,
                    "keyframes": keyframes,
                },
            )

        except LabellerrError as e:
            raise e
        except Exception as e:
            raise LabellerrError(f"Failed to delete key frames: {str(e)}")


LabellerrProjectMeta._register(DatasetDataType.video, VideoProject)
