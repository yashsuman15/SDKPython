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
import os

FILE_BATCH_SIZE=20 * 1024 * 1024
TOTAL_FILES_SIZE_LIMIT_PER_DATASET=2.5*1024*1024*1024
TOTAL_FILES_COUNT_LIMIT_PER_DATASET=2500
ANNOTATION_FORMAT=['json', 'coco_json', 'csv', 'png']

## DATA TYPES: image, video, audio, document, text
DATA_TYPES=('image', 'video', 'audio', 'document', 'text')
DATA_TYPE_FILE_EXT = {
    'image': ['.jpg','.jpeg', '.png', '.bmp', '.tiff'],
    'video': ['.mp4'],
    'audio': ['.mp3', '.wav'],
    'document': ['.pdf'],
    'text': ['.txt']
}
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
                'client_id': str(client_id),
                'content-type': 'application/json',
                'api_key':self.api_key,
                'api_secret': self.api_secret,
                'origin': 'https://dev.labellerr.com'
            }

            response = requests.request("POST", url, headers=headers, data=payload)

            if response.status_code != 200:
                raise LabellerrError(f"Project creation failed: {response.status_code} - {response.text}")
            print("rotation_config - - > ",rotation_config)
            if rotation_config is None:
                rotation_config = {
                    'annotation_rotation_count':1,
                    'review_rotation_count':1,
                    'client_review_rotation_count':1
                }

            self.rotation_config=rotation_config
            self.project_id=project_id
            self.client_id=client_id
                
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
            print(f"Update Rotation Count Payload: {payload}")

            response = requests.request("POST", url, headers=headers, data=payload)



            if response.status_code != 200:
                raise LabellerrError(f"Project rotation update config failed: {response.status_code} - {response.text}")

            return {'msg': 'project rotation configuration updated'}
        except LabellerrError as e:
            logging.error(f"Project rotation update config failed: {e}")
            raise

    def create_dataset(self,dataset_config):
        """
        Creates an empty dataset.

        :param dataset_config: A dictionary containing the configuration for the dataset.
        :return: A dictionary containing the response status and the ID of the created dataset.
        """
        try:
            # dataset_config['data_type'] has to be one of the items from DATA_TYPES
            # Validate data_type
            if dataset_config.get('data_type') not in DATA_TYPES:
                raise LabellerrError(f"Invalid data_type. Must be one of {DATA_TYPES}")
            dataset_id=f"dataset-{dataset_config['data_type']}-{uuid.uuid4().hex[:8]}"

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
                    "dataset_id": dataset_id,
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
                raise LabellerrError(f"dataset creation failed: {response.status_code} - {response.text}, request track id, {unique_id}")

            return {'response': 'success','dataset_id':dataset_id,'track_id':unique_id}

        except LabellerrError as e:
            e['track_id']=unique_id
            logging.error(f"Failed to create dataset: {e}")
            raise

    def get_all_dataset(self,client_id,datatype):
        """
        Retrieves a dataset by its ID.

        :param client_id: The ID of the client.
        :param datatype: The type of data for the dataset.
        :return: The dataset as JSON.
        """
        try:
            unique_id = str(uuid.uuid4())
            url = f"{self.base_url}/datasets/list?client_id={client_id}&data_type={datatype}&permission_level=project&project_id=null&uuid={unique_id}"
            headers = {
                'client_id': client_id,
                'content-type': 'application/json',
                'api_key': self.api_key,
                'api_secret': self.api_secret,
                'origin': 'https://dev.labellerr.com'
                }

            response = requests.request("GET", url, headers=headers)

            if response.status_code != 200:
                raise LabellerrError(f"dataset retrieval failed: {response.status_code} - {response.text}")

            return response.json()
        except LabellerrError as e:
            logging.error(f"Failed to retrieve dataset: {e}")
            raise

    def get_total_folder_file_count_and_total_size(self,folder_path,data_type):
        """
        Retrieves the total count and size of files in a folder.

        :param folder_path: The path to the folder.
        :param data_type: The type of data for the files.
        :return: The total count and size of the files.
        """
        total_file_count=0
        total_file_size=0
        files_list=[]
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                # print('>>  ',file_path)
                try:
                    # check if the file extention matching based on datatype
                    if not any(file.endswith(ext) for ext in DATA_TYPE_FILE_EXT[data_type]):
                        continue
                    files_list.append(file_path)
                    file_size = os.path.getsize(file_path)
                    total_file_count += 1
                    total_file_size += file_size
                except Exception as e:
                    print(f"Error reading {file_path}: {str(e)}")

        return total_file_count, total_file_size, files_list
    

    def get_total_file_count_and_total_size(self,files_list,data_type):
        """
        Retrieves the total count and size of files in a list.

        :param files_list: The list of file paths.
        :param data_type: The type of data for the files.
        :return: The total count and size of the files.
        """
        total_file_count=0
        total_file_size=0
        # for root, dirs, files in os.walk(folder_path):
        for file_path in files_list:
            if file_path is None:
                continue
            try:
                # check if the file extention matching based on datatype
                if not any(file_path.endswith(ext) for ext in DATA_TYPE_FILE_EXT[data_type]):
                    continue
                file_size = os.path.getsize(file_path)
                total_file_count += 1
                total_file_size += file_size
            except OSError as e:
                print(f"Error reading {file_path}: {str(e)}")
            except Exception as e:
                print(f"Unexpected error reading {file_path}: {str(e)}")

        return total_file_count, total_file_size, files_list


    def upload_folder_files_to_dataset(self, data_config):
        """
        Uploads local files to a dataset.

        :param data_config: A dictionary containing the configuration for the data.
        :return: A dictionary containing the response status and the list of successfully uploaded files.
        """
        try:
            unique_id = str(uuid.uuid4())
            url = f"{self.base_url}/connectors/upload/local?data_type={data_config['data_type']}&dataset_id={data_config['dataset_id']}&project_id=null&project_independent=false&client_id={data_config['client_id']}&uuid={unique_id}"
            data_config['url']=url
            files_list = []
            upload_queue=[]
            success_queue=[]
            fail_queue=[]

            total_file_count, total_file_volumn, filenames=self.get_total_folder_file_count_and_total_size(data_config['folder_path'],data_config['data_type'])
            
            if total_file_count>TOTAL_FILES_COUNT_LIMIT_PER_DATASET or total_file_volumn>TOTAL_FILES_SIZE_LIMIT_PER_DATASET:
                if total_file_count>TOTAL_FILES_COUNT_LIMIT_PER_DATASET:
                    raise LabellerrError(f"Total file count: {total_file_count} where limit is {TOTAL_FILES_COUNT_LIMIT_PER_DATASET} is too many file to upload")
                if total_file_volumn>TOTAL_FILES_SIZE_LIMIT_PER_DATASET:
                    raise LabellerrError(f"Total file size: {total_file_volumn/1024/1024:.1f}MB where the limit is {TOTAL_FILES_SIZE_LIMIT_PER_DATASET/1024/1024:.1f}MB is too large to upload")

            else:
                print(f"Total file count: {total_file_count}")
                print(f"Total file size: {total_file_volumn/1024/1024:.1f} MB")

                # for root, _, filenames in os.walk(data_config['folder_path']):
                #     print(f"commencing {len(filenames)} files uploading!")

                total_file_size=0

                for file_path in filenames:
                    filename = os.path.basename(file_path)

                    print('Reading file path...', file_path)
                    upload_queue.append(file_path)
                    print(f"cumulative file size {total_file_size} out of {FILE_BATCH_SIZE}")
                    try:
                        total_file_size += os.path.getsize(file_path)
                        if total_file_size < FILE_BATCH_SIZE:
                            print('Uploading more files to the batch')      

                            with open(file_path, 'rb') as file_obj:
                                files_list.append(
                                    ('file', (filename, file_obj.read(), 'application/octet-stream'))
                            )
                        else:

                            with open(file_path, 'rb') as file_obj:
                                files_list.append(
                                    ('file', (filename, file_obj.read(), 'application/octet-stream'))
                            )                            
                            print(f"Upload {len(files_list)} file(s) batch size {total_file_size/1024/1024:.1f}MB exceeds {FILE_BATCH_SIZE/1024/1024:.1f}MB")

                            response=self.commence_files_upload(data_config,files_list)
                            print(f"Response received: {response.status_code}")
                        
                            if response.status_code  != 200:
                                raise LabellerrError(f"Upload failed: {response.status_code} - {response.text}")
                            
                            success_queue.extend(upload_queue)
                            upload_queue = []
                            total_file_size=0
                            files_list=[]
                    except Exception as e:
                        print(f"Error opening file {file_path}: {str(e)}")
                        fail_queue.extend(upload_queue)
                        upload_queue = []
                        continue
                    


                if files_list and len(files_list)>0:
                    try:
                        response=self.commence_files_upload(data_config,files_list)
                        success_queue.append(upload_queue)
                    except Exception as e:
                        print(f"Error uploading files: {str(e)}")
                        fail_queue.append(upload_queue)
                    
            return {
                'track_id':unique_id,
                'success':success_queue,
                'fail':fail_queue
            }
        
        except Exception as e:
            raise LabellerrError(f"Failed to upload files: {str(e)}")
            
    

    def upload_files_to_dataset(self, data_config):
        """
        Uploads local files to a dataset.

        :param data_config: A dictionary containing the configuration for the data.
        :return: A dictionary containing the response status and the list of successfully uploaded files.
        """
        try:
            unique_id = str(uuid.uuid4())
            url = f"{self.base_url}/connectors/upload/local?data_type={data_config['data_type']}&dataset_id={data_config['dataset_id']}&project_id=null&project_independent=false&client_id={data_config['client_id']}&uuid={unique_id}"
            data_config['url']=url
            files_list = []
            upload_queue=[]
            success_queue=[]
            fail_queue=[]

            filenames=data_config['files_list']
            total_file_count, total_file_volumn, filenames=self.get_total_file_count_and_total_size(filenames,data_config['data_type'])
            
            if total_file_count>TOTAL_FILES_COUNT_LIMIT_PER_DATASET or total_file_volumn>TOTAL_FILES_SIZE_LIMIT_PER_DATASET:
                if total_file_count>TOTAL_FILES_COUNT_LIMIT_PER_DATASET:
                    raise LabellerrError(f"Total file count: {total_file_count} where limit is {TOTAL_FILES_COUNT_LIMIT_PER_DATASET} is too many file to upload")
                if total_file_volumn>TOTAL_FILES_SIZE_LIMIT_PER_DATASET:
                    raise LabellerrError(f"Total file size: {total_file_volumn/1024/1024:.1f}MB where the limit is {TOTAL_FILES_SIZE_LIMIT_PER_DATASET/1024/1024:.1f}MB is too large to upload")

            else:
                print(f"Total file count: {total_file_count}")
                print(f"Total file size: {total_file_volumn/1024/1024:.1f} MB")



            total_file_size=0

            for file_path in filenames:
                filename = os.path.basename(file_path)

                print('Reading file path...', file_path)
                upload_queue.append(file_path)
                print(f"cumulative file size {total_file_size} out of {FILE_BATCH_SIZE}")
                print('Upload Queue',upload_queue)
                try:
                    total_file_size += os.path.getsize(file_path)
                    if total_file_size < FILE_BATCH_SIZE:
                        print('Uploading more files to the batch')      

                        with open(file_path, 'rb') as file_obj:
                            files_list.append(
                                ('file', (filename, file_obj.read(), 'application/octet-stream'))
                        )
                    else:

                        with open(file_path, 'rb') as file_obj:
                            files_list.append(
                                ('file', (filename, file_obj.read(), 'application/octet-stream'))
                        )             
                        print(f"Upload {len(files_list)} file(s) batch size {total_file_size/1024/1024:.1f}MB exceeds {FILE_BATCH_SIZE/1024/1024:.1f}MB")
               
                        response=self.commence_files_upload(data_config,files_list)
                        print(f"Response received: {response.status_code}")
                    
                        if response.status_code  != 200:
                            raise LabellerrError(f"Upload failed: {response.status_code} - {response.text}")
                        
                        success_queue.extend(upload_queue)

                        upload_queue = []
                        total_file_size=0
                        files_list=[]
                except Exception as e:
                    print(f"Error opening file {file_path}: {str(e)}")
                    fail_queue.extend(upload_queue)
                    upload_queue = []
                    continue
                
        
            if files_list and len(files_list)>0:
                try:
                    print('before upload ',upload_queue)
                    response=self.commence_files_upload(data_config,files_list)
                    success_queue.extend(upload_queue)
                except Exception as e:
                    print(f"Error uploading files: {str(e)}")
                    print('pusing to failed queue',upload_queue)
                    fail_queue.extend(upload_queue)
                finally:
                    upload_queue = []
                    
            return {
                'track_id':unique_id,
                'success':success_queue,
                'fail':fail_queue
            }
        
        except Exception as e:
            raise LabellerrError(f"Failed to upload files: {str(e)}")
            
       


    def commence_files_upload(self,data_config,files_to_send):
        
        """
        Commences the upload of files to the API.

        :param data_config: The dictionary containing the configuration for the data.
        :param files_to_send: The list of files to send.
        :return: The response from the API.
        :raises LabellerrError: If the upload fails.
        """
        try:
            print(f"Uploading {len(files_to_send)} file(s)")
            headers = {
                    'client_id': data_config['client_id'],
                    'api_key': self.api_key,
                    'api_secret': self.api_secret,
                    'origin': 'https://dev.labellerr.com'
                }
            response=None
            response = requests.post(
                data_config['url'], 
                headers=headers, 
                data={}, 
                files=files_to_send
            )
            if response.status_code != 200:
                raise LabellerrError(f"Failed to upload files: {response.status_code} - {response.text}")
            return response
        except requests.exceptions.RequestException as e:
            raise LabellerrError(f"Request failed: {str(e)}")
        except Exception as e:
            raise LabellerrError(f"An error occurred during file upload: {str(e)}")
        finally:
            # Ensure all files are closed
            for file_obj in files_to_send:
                try:
                    file_obj.close()
                except:
                    pass

    def upload_files(self,client_id,dataset_id,data_type,files_list):

        """
        Uploads files to the API.

        :param client_id: The ID of the client.
        :param dataset_id: The ID of the dataset.
        :param data_type: The type of data.
        :param files_list: The list of files to upload.
        :return: The response from the API.
        :raises LabellerrError: If the upload fails.
        """
        try:
            # convert commaseparated string to list from files_list
            files_list = files_list.split(',')
            if(len(files_list)==0):
                raise LabellerrError("No files to upload")
            
            config = {
                'client_id': client_id,
                'dataset_id': dataset_id,
                'project_id': 'null',
                'data_type': data_type,
                'files_list': files_list,
                'project_independent':'false'
            }

            response = self.upload_files_to_dataset(config)
            
            return response
        except Exception as e:
            logging.error(f"Failed to upload files : {str(e)}")
            raise LabellerrError(f"Failed to upload files : {str(e)}")

    

    def upload_folder_content(self,client_id,dataset_id,data_type,folder_path):

        """
        Uploads the content of a folder to the API.

        :param client_id: The ID of the client.
        :param dataset_id: The ID of the dataset.
        :param data_type: The type of data.
        :param folder_path: The path to the folder.
        :return: The response from the API.
        :raises LabellerrError: If the upload fails.
        """
        
        try:
            if not os.path.exists(folder_path) or not os.path.isdir(folder_path) or not os.listdir(folder_path):
                raise LabellerrError("Invalid or empty folder path")
            
            config = {
                'client_id': client_id,
                'dataset_id': dataset_id,
                'project_id': 'null',
                'data_type': data_type,
                'folder_path': folder_path,
                'project_independent':'false'
            }

            response = self.upload_folder_files_to_dataset(config)
            
            return response
        except Exception as e:
            logging.error(f"Failed to upload folder content: {str(e)}")
            raise LabellerrError(f"Failed to upload folder content: {str(e)}")
        
    
        

    def get_all_project_per_client_id(self,client_id):

        """
        Retrieves a list of projects associated with a client ID.

        :param client_id: The ID of the client.
        :return: A dictionary containing the list of projects.
        :raises LabellerrError: If the retrieval fails.
        """
        try:
            unique_id = str(uuid.uuid4())
            url = f"{self.base_url}/project_drafts/projects/detailed_list?client_id={client_id}&uuid={unique_id}"

            payload = {}
            headers = {
                'client_id': str(client_id),
                'content-type': 'application/json',
                'api_key': self.api_key,
                'api_secret': self.api_secret,
                'origin': 'https://dev.labellerr.com'
            }

            response = requests.request("GET", url, headers=headers, data=payload)

            print(response.text)
            return response.json()
        except Exception as e:
            logging.error(f"Failed to retrieve projects: {str(e)}")
            raise LabellerrError(f"Failed to retrieve projects: {str(e)}")

    
                        
    def link_dataset_to_project(self, client_id,project_id,dataset_id):
        """
        Links a dataset to a project.

        :param client_id: The ID of the client.
        :param project_id: The ID of the project.
        :param dataset_id: The ID of the dataset.
        :return: The response from the API.
        :raises LabellerrError: If the linking fails.
        """
        try:
            unique_id = str(uuid.uuid4())
            url = f"{self.base_url}/datasets/project/link?client_id={client_id}&dataset_id={dataset_id}&project_id={project_id}&uuid={unique_id}"

            payload = {}
            
            headers = {
                'client_id': str(client_id),
                'content-type': 'application/json',
                'api_key': self.api_key,
                'api_secret': self.api_secret,
                'origin': 'https://dev.labellerr.com'
            }            

            response = requests.request("GET", url, headers=headers, data=payload)
            response=response.json()
            response['track_id'] = unique_id
            print(response)
            return response
        except Exception as e:
            logging.error(f"Failed to link the data with the projects :{str(e)}")
            raise LabellerrError(f"Failed to link the data with the projects : {str(e)}")
    



    def update_project_annotation_guideline(self,config):

        """
        Updates the annotation guideline for a project.

        :param config: A dictionary containing the project ID, data type, client ID, autolabel status, and the annotation guideline.
        :return: None
        :raises LabellerrError: If the update fails.
        """
        unique_id = str(uuid.uuid4())

        url = f"{self.base_url}/annotations/add_questions?project_id={config['project_id']}&auto_label={config['autolabel']}&data_type={config['data_type']}&client_id={config['client_id']}&uuid={unique_id}"

        guide_payload = json.dumps(config['annotation_guideline'])
        
        headers = {
            'client_id': str(config['client_id']),
            'content-type': 'application/json',
            'api_key': self.api_key,
            'api_secret': self.api_secret,
            'origin': 'https://dev.labellerr.com'
        }    

        print('annotation_guide -- ', guide_payload)
        try:
            response = requests.request("POST", url, headers=headers, data=guide_payload)
            print(' guideline update  ',response)
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to update project annotation guideline: {str(e)}")
            raise LabellerrError(f"Failed to update project annotation guideline: {str(e)}")
        
    

    def validate_rotation_config(self,rotation_config):
        """
        Validates a rotation configuration.

        :param rotation_config: A dictionary containing the configuration for the rotations.
        :raises LabellerrError: If the configuration is invalid.
        """
        annotation_rotation_count = rotation_config.get('annotation_rotation_count')
        review_rotation_count = rotation_config.get('review_rotation_count')
        client_review_rotation_count = rotation_config.get('client_review_rotation_count')

        # Validate review_rotation_count
        if review_rotation_count != 1:
            raise LabellerrError("review_rotation_count must be 1")

        # Validate client_review_rotation_count based on annotation_rotation_count
        if annotation_rotation_count == 0 and client_review_rotation_count != 0:
            raise LabellerrError("client_review_rotation_count must be 0 when annotation_rotation_count is 0")
        elif annotation_rotation_count == 1 and client_review_rotation_count not in [0, 1]:
            raise LabellerrError("client_review_rotation_count can only be 0 or 1 when annotation_rotation_count is 1")
        elif annotation_rotation_count > 1 and client_review_rotation_count != 0:
            raise LabellerrError("client_review_rotation_count must be 0 when annotation_rotation_count is greater than 1")


    def upload_preannotation_by_project_id(self,project_id,client_id,annotation_format,annotation_file):

        """
        Uploads preannotation data to a project.

        :param project_id: The ID of the project.
        :param client_id: The ID of the client.
        :param annotation_format: The format of the preannotation data.
        :param annotation_file: The file path of the preannotation data.
        :return: The response from the API.
        :raises LabellerrError: If the upload fails.
        """
        try:
            # validate all the parameters
            required_params = ['project_id', 'client_id', 'annotation_format', 'annotation_file']
            for param in required_params:
                if param not in locals():
                    raise LabellerrError(f"Required parameter {param} is missing")
                
            if annotation_format not in ANNOTATION_FORMAT:
                raise LabellerrError(f"Invalid annotation_format. Must be one of {ANNOTATION_FORMAT}")
            

            url = f"{self.base_url}/actions/upload_answers?project_id={project_id}&answer_format={annotation_format}&client_id={client_id}"

            # validate if the file exist then extract file name from the path
            if os.path.exists(annotation_file):
                file_name = os.path.basename(annotation_file)
            else:
                raise LabellerrError("File not found")

            payload = {}
            # with open(annotation_file, 'rb') as f:
            #     files = [('file', (file_name, f, 'application/json'))]
            files=[
                ('file',(file_name,open(annotation_file,'rb'),'application/octet-stream'))
                ]

            headers = {
            'client_id': client_id,
            'api_key': self.api_key,
            'api_secret': self.api_secret,
            'origin': 'https://dev.labellerr.com',
            'email_id': self.api_key
            }

            response = requests.request("POST", url, headers=headers, data=payload, files=files)

            print(response.text)
            if response.status_code != 200:
                raise LabellerrError(f"Failed to upload preannotation: {response.text}")
            
            return response.json()
        except Exception as e:
            logging.error(f"Failed to upload preannotation: {str(e)}")
            raise LabellerrError(f"Failed to upload preannotation: {str(e)}")


    def create_local_export(self,project_id,client_id,export_config):

        required_params = ['project_id', 'client_id', 'export_config']
        for param in required_params:
            if param not in locals():
                raise LabellerrError(f"Required parameter {param} is missing")

        

        url=f"{self.base_url}/sdk_python/export/files?project_id={project_id}&client_id={client_id}"
        payload = json.dumps(export_config)
        headers = {
        'api_key': 'f08d49.85f21d405680fd3460ffa2bdc9',
        'api_secret': 'c8ddb6d24bc07242543d56e688edf57efcf98a01387fcc4a7c46441a9ac198d6',
        'Origin': 'https://dev.labellerr.com',
        'Content-Type': 'application/json'
        }

        try:
            response = requests.request("POST", url, headers=headers, data=payload)
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to create local export: {str(e)}")
            raise LabellerrError(f"Failed to create local export: {str(e)}")

    def initiate_create_project(self,payload):

        # Creating an empty dataset by function call
        """
        Creates an empty project.

        :param payload: A dictionary containing the configuration for the project.
        :return: A dictionary containing the dataset ID, project ID, and project configuration.
        """
        
        try:
            result={}
            print('Payload  >>> ',payload)

            # validate all the parameters
            required_params = ['client_id', 'dataset_name', 'dataset_description', 'data_type', 'created_by', 'project_name','annotation_guide','autolabel']
            for param in required_params:
                if param not in payload:
                    raise LabellerrError(f"Required parameter {param} is missing")
                if(param == 'client_id'):
                    # it should be an instance of string
                    if not isinstance(payload[param], str) or not payload[param].strip():
                        raise LabellerrError(f"client_id must be a string")
                
        
            if 'rotation_config' in payload:
                self.validate_rotation_config(payload['rotation_config'])
            else:
                payload['rotation_config'] = {
                    'annotation_rotation_count':1,
                    'review_rotation_count':1,
                    'client_review_rotation_count':1
                }
            
            if payload['data_type'] not in DATA_TYPES:
                raise LabellerrError(f"Invalid data_type. Must be one of {DATA_TYPES}")

            if 'files_to_upload' in payload and 'folder_to_upload' in payload:
                raise LabellerrError("Both files_to_upload and folder_to_upload cannot be provided at the same time.")
            elif 'files_to_upload' not in payload and 'folder_to_upload' not in payload:
                raise LabellerrError("Either files_to_upload or folder_to_upload must be provided.")
            else:
                if 'files_to_upload' in payload:
                    if payload['files_to_upload'] is None and len(payload['files_to_upload'])==0:
                        raise LabellerrError("files_to_upload must be a non-empty string.")
                elif 'folder_to_upload' in payload:
                    if not isinstance(payload['folder_to_upload'], str) or not payload['folder_to_upload'].strip():
                        raise LabellerrError("folder_to_upload must be a non-empty string.")
                

            response = self.create_dataset({
                'client_id': payload['client_id'],
                'dataset_name': payload['project_name'],
                'data_type': payload['data_type'],
                'dataset_description': payload['dataset_description'],
                'created_by': payload['created_by']
            })
            print('Dataset creation response ',response)

            dataset_id = response['dataset_id']
            result['dataset_id'] = dataset_id

            # now upload local files/folder to dataset
            if 'files_to_upload' in payload and payload['files_to_upload'] is not None:
                data=self.upload_files_to_dataset({
                    'client_id': payload['client_id'],
                    'dataset_id': dataset_id,
                    'data_type': payload['data_type'],
                    'files_list': payload['files_to_upload']
                })
                result['dataset_files'] = data

            elif 'folder_to_upload' in payload and payload['folder_to_upload'] is not None:
                data=self.upload_folder_files_to_dataset({
                    'client_id': payload['client_id'],
                    'dataset_id': dataset_id,
                    'data_type': payload['data_type'],
                    'folder_path': payload['folder_to_upload']
                })
                result['dataset_files'] = data

            # create empty project
            response = self.create_empty_project(payload['client_id'], payload['project_name'], payload['data_type'],payload['rotation_config'])

            project_id = response['project_id']
            result['project_id'] = project_id
            result['project_config'] = response['project_config']   

            # update the project annotation guideline 
            # if 'annotation_guide' in payload and 'autolabel' in payload:
            try:
                guideline={
                    "project_id":project_id,
                    "client_id":payload['client_id'],
                    "autolabel":payload['autolabel'],
                    "data_type": payload['data_type'],
                    "annotation_guideline":payload['annotation_guide']
                }
                guideline_update=self.update_project_annotation_guideline(guideline)
                result['annotation_guide']=guideline_update
            except Exception as e:
                logging.error(f"Failed to update project annotation guideline: {str(e)}")
                print(result)
                raise LabellerrError(f"Failed to update project annotation guideline: {str(e)}")
        

            # link dataset to project
            data=self.link_dataset_to_project(payload['client_id'],project_id,dataset_id)
            result['dataset_project_link'] = data
            result['response'] = 'success'
            return result
        except Exception as e:
            logging.error(f"Failed to create project: {str(e)}")
            print(result)
            raise LabellerrError(f"Failed to create project: {str(e)}")
        

