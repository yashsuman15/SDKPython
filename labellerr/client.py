# labellerr/client.py

import requests
import uuid
from .exceptions import LabellerrError
from unique_names_generator import get_random_name
from unique_names_generator.data import ADJECTIVES, NAMES, ANIMALS
import random
import json
import logging 
from datetime import datetime 


## DATA TYPES: image, video, audio, document, text
# python -m unittest discover -s tests

create_dataset_parameters={}

class LabellerrClient:
    """
    A client for interacting with the Labellerr API.
    """
    def __init__(self, api_key, api_secret):
        """
        Initializes the LabellerrClient with API credentials.

        :param api_key: The API key for authentication.
        :param api_secret: The API secret for authentication.
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api-gateway-qcb3iv2gaa-uc.a.run.app" #--dev
        # self.base_url = "https://api.labellerr.com" #--prod

    def get_dataset(self, workspace_id, dataset_id, project_id):
        """
        Retrieves a dataset from the Labellerr API.

        :param workspace_id: The ID of the workspace.
        :param dataset_id: The ID of the dataset.
        :param project_id: The ID of the project.
        :return: The dataset as JSON.
        """
        url = f"{self.base_url}?client_id={workspace_id}&dataset_id={dataset_id}&project_id={project_id}&uuid={str(uuid.uuid4())}"
        headers = {
            'api_key': self.api_key,
            'api_secret': self.api_secret,
            'Origin': 'https://pro.labellerr.com'
        }
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise LabellerrError(f"Error {response.status_code}: {response.text}")
        return response.json()

    # def get_file(self, project_id, client_id, email_id, uuid):
    #     """
    #     Retrieves a file from the Labellerr API.

    #     :param project_id: The ID of the project.
    #     :param client_id: The ID of the client.
    #     :param email_id: The email ID of the client.
    #     :param uuid: The unique identifier of the file.
    #     :return: The file as JSON.
    #     """
    #     url = f"{self.base_url}/data/file/v1?status=assigned&cluster_id=undefined&exceptions_list=014beb4e-80f7-4d41-a451-2ec7f6dffe32&project_id={project_id}&uuid={uuid}&client_id={client_id}"
    #     headers = {
    #         'email_id': email_id,
    #         'client_id': client_id,
    #         'api_key': self.api_key,
    #         'api_secret': self.api_secret,
    #         'Origin': 'https://sumittest.labellerr.com',
    #         # 'Authorization': 'Bearer <your_token_here>'  # Replace with actual token if needed
    #     }
    #     response = requests.get(url, headers=headers)
    #     if response.status_code != 200:
    #         raise LabellerrError(f"Error {response.status_code}: {response.text}")
    #     return response.json()

    def create_empty_project(self, client_id, project_name, data_type, rotation_config=None):
        """
        Creates an empty project on the Labellerr API.

        :param client_id: The ID of the client.
        :param project_name: The name of the project.
        :param data_type: The type of data for the project.
        :param rotation_config: The rotation configuration for the project.
        :return: A dictionary containing the project ID, response status, and project configuration.
        """
        try:
            unique_id = str(uuid.uuid4())
            url = f"{self.base_url}/projects/create?stage=0&client_id={client_id}&uuid={unique_id}"

            project_id = get_random_name(combo=[NAMES, ADJECTIVES, ANIMALS], separator="_", style="lowercase") + '_' + str(random.randint(10000, 99999))

            payload = json.dumps({
                "project_id": project_id,
                "project_name": project_name,
                "data_type": data_type
            })

            print(f"Create Empty Project Payload: {payload}")

            headers = {
                'client_id': client_id,
                'content-type': 'application/json',
                'api_key': self.api_key,
                'api_secret': self.api_secret,
                'origin': 'https://dev.labellerr.com'
            }

            response = requests.request("POST", url, headers=headers, data=payload)

            if rotation_config is None:
                rotation_config = {
                    'annotation_rotation_count':1,
                    'review_rotation_count':0,
                    'client_review_rotation_count':0
                }

            self.rotation_config=rotation_config
            self.project_id=project_id
            self.client_id=client_id

            if response.status_code != 200:
                raise LabellerrError(f"Project creation failed: {response.status_code} - {response.text}")

            rotation_request_response=self.update_rotation_count()

            return {'project_id': project_id, 'response': 'success','project_config':rotation_request_response}
        except LabellerrError as e:
            logging.error(f"Failed to create project: {e}")
            raise

    def update_rotation_count(self):
        """
        Updates the rotation count for a project.

        :return: A dictionary indicating the success of the operation.
        """
        try:
            unique_id = str(uuid.uuid4())
            url = f"{self.base_url}/projects/rotations/add?project_id={self.project_id}&client_id={self.client_id}&uuid={unique_id}"

            headers = {
                'client_id': self.client_id,
                'content-type': 'application/json',
                'api_key': self.api_key,
                'api_secret': self.api_secret,
                'origin': 'https://dev.labellerr.com'
                }

            payload = json.dumps(self.rotation_config)

            response = requests.request("POST", url, headers=headers, data=payload)

            if response.status_code != 200:
                raise LabellerrError(f"Project creation failed: {response.status_code} - {response.text}")

            return {'msg': 'project rotation configuration updated'}
        except LabellerrError as e:
            logging.error(f"Failed to create project: {e}")
            raise

    def create_dataset(self,dataset_config):
        """
        Creates an empty dataset.
        """
        try:
            unique_id = str(uuid.uuid4())
            url = f"{self.base_url}/datasets/create?client_id={dataset_config['client_id']}&uuid={unique_id}"
            headers = {
                'client_id': str(dataset_config['client_id']),
                'content-type': 'application/json',
                'api_key': self.api_key,
                'api_secret': self.api_secret,
                'origin': 'https://dev.labellerr.com'
                }

            payload = json.dumps(
                {
                    "dataset_id": f"dataset-{dataset_config['data_type']}-{uuid.uuid4().hex[:8]}",
                    "dataset_name": dataset_config['dataset_name'],
                    "dataset_description": dataset_config['dataset_description'],
                    "data_type": dataset_config['data_type'],
                    "created_by": dataset_config['created_by'],
                    "permission_level": "project",
                    "type": "client",
                    "labelled": "unlabelled",
                    "data_copy": "false",
                    "isGoldDataset": False,
                    "files_count": 0,
                    "access": "write",
                    "created_at": datetime.now().isoformat(),
                    "id": f"dataset-{dataset_config['data_type']}-{uuid.uuid4().hex[:8]}",
                    "name": dataset_config['dataset_name'],
                    "description": dataset_config['dataset_description']
                }
            )

            response = requests.request("POST", url, headers=headers, data=payload)

            if response.status_code != 200:
                raise LabellerrError(f"dataset creation failed: {response.status_code} - {response.text}")

            return {'response': 'success'}

        except LabellerrError as e:
            logging.error(f"Failed to create dataset: {e}")
            raise