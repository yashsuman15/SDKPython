import unittest
from labellerr.client import LabellerrClient
from labellerr.exceptions import LabellerrError
import json
import uuid
# python -m unittest discover -s tests
class TestLabellerrClient(unittest.TestCase):
    def setUp(self):
        # self.client = LabellerrClient('f483bd.53bac54d36b8382a176138d46c', 'c9d7255834ca6b67f55a6c5f60121d4fe7e47851c022bc0237fecda541ab0eae') #--dev
        self.client = LabellerrClient('f08d49.85f21d405680fd3460ffa2bdc9', 'c8ddb6d24bc07242543d56e688edf57efcf98a01387fcc4a7c46441a9ac198d6') #--prod

    # def test_get_file(self):
    #     project_id = 'veronike_loose_raven_39117'
    #     client_id = '8748'
    #     email_id = 'angansen@gmail.com'
    #     uuid = '6ca9ff2c-a500-4125-8912-f74351ba9c9d'
    #     try:
    #         result = self.client.get_file(project_id, client_id, email_id, uuid)
    #         print(json.dumps(result, indent=4))  # Log the return value to the console in a pretty JSON format
    #     except LabellerrError as e:
    #         print(f"An error occurred: {e}")  # Log the error message

    def test_create_project(self):
        client_id = '1'
        project_name = 'Test Project2'
        data_type = 'image'
        rotation_config = {
            'annotation_rotation_count': 0,
            'review_rotation_count': 1,
            'client_review_rotation_count': 0
        }

        try:
            result = self.client.create_empty_project(
                client_id=client_id,
                project_name=project_name,
                data_type=data_type,
                rotation_config=rotation_config
            )

            # verify that the result pattern is like this: {'project_id': 'veronike_loose_raven_39117','response': 'success'}
            self.assertTrue(isinstance(result, dict) and len(result) == 3 and 'project_id' in result and 'response' in result)
            self.assertEqual(result['response'], 'success')

            # verify that the project_id is not empty
            self.assertTrue(result['project_id'])

            # Log the return value
            print(f"Project create api response: {result}")

            
        except LabellerrError as e:
            print(f"An error occurred: {e}")
            raise  # Re-raise the exception to fail the test

    def test_create_dataset(self):

        payload={
                "client_id":1,
                "dataset_name": 'Sample',
                "dataset_description": 'sample description',
                "data_type": "image",
                "created_by":'angansen@gmail.com',
                "permission_level": "project",
                "type": "client",
                "labelled": "unlabelled",
                "data_copy": "false",
                "isGoldDataset": False,
                "files_count": 0,
                "access": "write"
            }
        
        try:
            result = self.client.create_dataset(payload)
            self.assertEqual(result['response'], 'success')

            # Log the return value
            print(f"Dataset create api response: {result}")

        except LabellerrError as e:
            print(f"An error occurred: {e}")
            raise

if __name__ == '__main__':
    unittest.main()

