# labellerr/client.py

import requests
import uuid
from .exceptions import LabellerrError

class LabellerrClient:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.labellerr.com/datasets/project/link"

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
        url = f"https://api.labellerr.com/data/file/v1?status=assigned&cluster_id=undefined&exceptions_list=014beb4e-80f7-4d41-a451-2ec7f6dffe32&project_id={project_id}&uuid={uuid}&client_id={client_id}"
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
