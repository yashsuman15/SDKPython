# labellerr/client.py

import requests
import uuid
from .exceptions import LabellerrError
from unique_names_generator import get_random_name
from unique_names_generator.data import ADJECTIVES, NAMES, ANIMALS
import random
import json
import logging 

## DATA TYPES: image, video, audio, document, text
# python -m unittest discover -s tests

class LabellerrClient:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api-gateway-qcb3iv2gaa-uc.a.run.app" #--dev
        # self.base_url = "https://api.labellerr.com" #--prod

    def get_dataset(self, workspace_id, dataset_id, project_id):
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

    def get_file(self, project_id, client_id, email_id, uuid):
        url = f"{self.base_url}/data/file/v1?status=assigned&cluster_id=undefined&exceptions_list=014beb4e-80f7-4d41-a451-2ec7f6dffe32&project_id={project_id}&uuid={uuid}&client_id={client_id}"
        
        
        headers = {
            'email_id': email_id,
            'client_id': client_id,
            'api_key': self.api_key,
            'api_secret': self.api_secret,
            'Origin': 'https://sumittest.labellerr.com',
            # 'Authorization': 'Bearer <your_token_here>'  # Replace with actual token if needed
        }
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise LabellerrError(f"Error {response.status_code}: {response.text}")
        return response.json()

    def create_empty_project(self, client_id,project_name, data_type, rotation_config=None):
        try:

            # url = "https://api-gateway-qcb3iv2gaa-uc.a.run.app/projects/create?stage=0&client_id=1&uuid=693827e0-12a8-4124-9619-bbf23c6b29f2"            # unique_id = str(uuid.uuid4())
            
            unique_id = str(uuid.uuid4())
            url = f"https://api-gateway-qcb3iv2gaa-uc.a.run.app/projects/create?stage=0&client_id={client_id}&uuid={unique_id}"            # unique_id = str(uuid.uuid4())

            # url = f"{self.base_url}/project/create?stage=0&client_id={client_id}&uuid={unique_id}"
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

            if response.status_code != 200:
                raise LabellerrError(f"Project creation failed: {response.status_code} - {response.text}")

            return {'project_id': project_id, 'response': 'success'}
        except LabellerrError as e:
            logging.error(f"Failed to create project: {e}")
            raise


    # def create_project(self, client_id, project_name, data_type, rotation_config=None):
    #     """
    #     Create a new project with the specified name and data type.
        
    #     Args:
    #         client_id (str): Client ID on successful authentication
    #         project_name (str): Name of the project
    #         data_type (str): Type of data for the project
    #         rotation_config (dict, optional): Configuration for annotation rotations
    #             {
    #                 'annotation_rotation_count': int,
    #                 'review_rotation_count': int,
    #                 'client_review_rotation_count': int
    #             }
    #     """
    #     unique_id = str(uuid.uuid4())
    #     url = f"{self.base_url}/project/create?client_id={client_id}&uuid={unique_id}"
    #     headers = {
    #         'Origin': 'https://pro.labellerr.com',
    #         'api_key': self.api_key,
    #         'api_secret': self.api_secret,
    #         'email_id': self.api_key,
    #         'client_id': client_id,
    #         'Content-Type': 'application/json'
    #     }
        
    #     payload = {
    #         "project_id": get_random_name(combo=[NAMES, ADJECTIVES, ANIMALS], separator="_", style="lowercase") + '_' + str(random.randint(10000, 99999)),  # Generate project ID
    #         "project_name": project_name,
    #         "data_type": data_type
    #     }
    #     print("Create Project Payload: ", payload)
    #     print("Create Project URL: ", url)
    #     response = requests.post(url, headers=headers, json=payload)
    #     if response.status_code != 200:
    #         raise LabellerrError(f"Project creation failed: {response.status_code} - {response.text}")
            
    #     project_id = payload["project_id"]
        
    #     # Update project configurations if provided
    #     if rotation_config:
    #         config_url = f"{self.base_url}project/configurations/update?client_id={client_id}&project_id={project_id}&uuid={unique_id}"
            
    #         config_payload = {
    #             "annotation_rotation_count": rotation_config.get('annotation_rotation_count', 1),
    #             "review_rotation_count": rotation_config.get('review_rotation_count', 0),
    #             "client_review_rotation_count": rotation_config.get('client_review_rotation_count', 0)
    #         }
    #         print("Config Payload: ", config_payload)
    #         print("Config URL: ", config_url)
    #         config_response = requests.post(config_url, headers=headers, json=config_payload)
    #         if config_response.status_code != 200:
    #             raise LabellerrError(f"Failed to update project configurations: {config_response.status_code} - {config_response.text}")
        
    #     return {"project_id": project_id}