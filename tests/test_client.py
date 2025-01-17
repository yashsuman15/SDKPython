import unittest
from labellerr.client import LabellerrClient
from labellerr.exceptions import LabellerrError
import json
import uuid
import threading
import time
# RUNNING
# python -m unittest discover -s tests

# BULDING
# python setup.py sdist bdist_wheel 

class TestLabellerrClient(unittest.TestCase):
    def setUp(self):
        self.client = LabellerrClient('64d61b.90c2cc4de6a8be69d2d32ffaeb', 'baa3d611f7780faf9d263c2857a57fc356a061fbde44e41820f066002a068dfc') #--dev
        # self.client = LabellerrClient('2006c6.c9a76949feafa3a5e141360e3e', '89eb2f5ddff2114c79fdef17dffd29a1a209f1ac3f40a69cddb17d9f0bc48037') #--prod
    
    

    # def test_create_project(self):
    #     client_id = '1'
    #     project_name = 'Test Project2'
    #     data_type = 'image'
    #     rotation_config = {
    #         'annotation_rotation_count': 0,
    #         'review_rotation_count': 1,
    #         'client_review_rotation_count': 0
    #     }

    #     try:
    #         result = self.client.create_empty_project(
    #             client_id=client_id,
    #             project_name=project_name,
    #             data_type=data_type,
    #             rotation_config=rotation_config
    #         )

    #         # verify that the result pattern is like this: {'project_id': 'veronike_loose_raven_39117','response': 'success'}
    #         self.assertTrue(isinstance(result, dict) and len(result) == 3 and 'project_id' in result and 'response' in result)
    #         self.assertEqual(result['response'], 'success')

    #         # verify that the project_id is not empty
    #         self.assertTrue(result['project_id'])

    #         # Log the return value
    #         print(f"Project create api response: {result}")

            
    #     except LabellerrError as e:
    #         print(f"An error occurred: {e}")
    #         raise  # Re-raise the exception to fail the test




    # def test_create_dataset(self):

    #     payload={
    #             "client_id":1,
    #             "dataset_name": 'Sample',
    #             "dataset_description": 'sample description',
    #             "data_type": "image",
    #             "created_by":'angansen@gmail.com',
    #             "permission_level": "project",
    #             "type": "client",
    #             "labelled": "unlabelled",
    #             "data_copy": "false",
    #             "isGoldDataset": False,
    #             "files_count": 0,
    #             "access": "write"
    #         }
        
    #     try:
    #         result = self.client.create_dataset(payload)
    #         self.assertEqual(result['response'], 'success')

    #         # Log the return value
    #         print(f"Dataset create api response: {result}")

    #     except LabellerrError as e:
    #         print(f"An error occurred: {e}")
    #         raise


    # def test_initiate_project(self):
    #     try:
    #         payload={
    #             # -----  Create empty dataset object   --------
    #             "client_id":'1',
    #             "dataset_name": 'Sample',
    #             "data_type": "image",
    #             "created_by":'angansen@gmail.com',
    #             "dataset_description": 'sample description',
    #             "autolabel":"false",
    #             # -----    Local Folder upload to dataset object   --------
    #             "folder_to_upload": '/Users/angansen/Documents/labelerr/test_image',
    #             # "files_to_upload":['/Users/angansen/Documents/labelerr/test_image/female/6890.jpg', '/Users/angansen/Documents/labelerr/test_image/female/6898.jpg', '/Users/angansen/Documents/labelerr/test_image/female/7416.jpg'],
    #             # ------ create empty project object   --------
    #             "project_name":'Test Project2',
    #             "annotation_guide":[
    #                                     {
    #                                         "question_number": 1,
    #                                         "question": "Test4",
    #                                         "required": 'false',
    #                                         "options": [
    #                                             {
    #                                                 "option_name": "#4682B4"
    #                                             }
    #                                         ],
    #                                         "question_id": "533bb0c8-fb2b-4394-a8e1-5042a944802f",
    #                                         "option_type": "BoundingBox",
    #                                         "question_metadata": []
    #                                     }
    #                                 ],
    #             "rotation_config":{
    #                 'annotation_rotation_count': 0,
    #                 'review_rotation_count': 1,
    #                 'client_review_rotation_count': 0
    #             }
    #         }

    #         result = self.client.initiate_create_project(payload)
    #         self.assertEqual(result['response'], 'success')

    #         # Log the return value
    #         print(f"Project initiate api response: {result}")


    #     except LabellerrError as e:
    #         print(f"An error occurred: {e}")
    #         raise



    # upload pre annotation file
    def test_preannotation_file_by_project_id(self):
        try:
            annotation_file = '/Users/angansen/Documents/labelerr/_annotations_2500_images.json'
            client_id = '1'
            project_id='renee_smooth_frog_20413'
            annotation_format='coco_json'
            
            # Start the async operation
            future = self.client.upload_preannotation_by_project_id_async(project_id,client_id,annotation_format,annotation_file)
            
            # Optional: wait for completion at the end of test
            try:
                result = future.result(timeout=300)  # 5 minutes timeout
                self.assertTrue('error' not in result)
                self.assertTrue('response' in result)
                self.assertTrue('status' in result['response'])
                self.assertEqual(result['response']['status'], 'completed')
            except Exception as e:
                print(f"Error in future execution: {str(e)}")
                raise
            
        except Exception as e:
            print(f"An error occurred: {e}")
            raise




    # def test_local_export(self):
    #         """
    #         Test uploading multiple files from a folder to a dataset.
    #         /Users/angansen/Documents/labelerr/test_data
    #         """
    #         # Test configuration
    #         client_id = '1'
    #         project_id='vivien_just_horse_78859'
    #         export_config={
    #         "export_name": "Test",
    #         "export_description": "Test export",
    #         "export_format": "coco_json",
    #         "statuses": [
    #             "review"
    #         ]
    #         }
    #         result=self.client.create_local_export(project_id,client_id,export_config)

    #         # result should not have error
    #         self.assertTrue('error' not in result)
        
    #         # Log the validation result
    #         print("Validation local upload: SUCCESS", result)



    # def test_folder_upload_dataset(self):
    #         """
    #         Test uploading multiple files from a folder to a dataset.
    #         """
    #         # Test configuration
    #         upload_folder = '/Users/angansen/Documents/labelerr/test_image/'  # Create this folder and add some test images
    #         client_id = '1'
    #         dataset_id = 'dataset-image-c42bfeab-a111-4c03-a07b-1'
    #         data_type = 'image'
    #         result=self.client.upload_folder_content(client_id,dataset_id,data_type,upload_folder)

    #         self.assertTrue('success' in result)
    #         self.assertTrue('fail' in result)
    #         self.assertTrue(isinstance(result['success'], list))
    #         self.assertTrue(isinstance(result['fail'], list))
           
    #         # Log the validation result
    #         print("Validation of folder upload dataset result: SUCCESS")





    # def test_files_upload_dataset(self):
    #     """
    #     Test uploading multiple files from a folder to a dataset.
    #     """
    #     # Test configuration
    #     files = '/Users/angansen/Documents/labelerr/test_image/female copy 3/shoes copy 5/12890.jpg,/Users/angansen/Documents/labelerr/test_image/female copy 3/shoes copy 5/19866.jpg,/Users/angansen/Documents/labelerr/test_image/female copy 3/shoes copy 5/15721.jpg'  # Create this folder and add some test images
    #     client_id = '1'
    #     dataset_id = 'dataset-image-c42bfeab-a111-4c03-a07b-1'
    #     data_type = 'image'
    #     try:
    #         result = self.client.upload_files(client_id,dataset_id,data_type,files)
    #         self.assertTrue('success' in result)
    #         self.assertTrue('fail' in result)
    #         self.assertTrue(isinstance(result['success'], list))
    #         self.assertTrue(isinstance(result['fail'], list))
    #         # Log the validation result
    #         print("Validation of files upload dataset result: SUCCESS")

    #     except LabellerrError as e:
    #         print(f"An error occurred: {e}")
    #         raise





    # def test_get_all_datasets_by_client_id(self):
    #     """
    #     Test retrieving all datasets by a client's ID.
    #     """
    #     # Test configuration
    #     client_id = '1'  # Replace with a valid client ID
    #     data_type = 'image'  # Retrieve all datasets regardless of data type

    #     try:
    #         result = self.client.get_all_dataset(client_id, data_type)
    #         self.assertTrue(result)  # Ensure the result is not empty
    #         self.assertTrue('linked' in result)
    #         self.assertTrue('unlinked' in result)
    #         self.assertTrue(isinstance(result['linked'], list))
    #         self.assertTrue(isinstance(result['unlinked'], list))
    #         # Log the validation result
    #         print("Validation of get all datasets by client ID result: SUCCESS")

    #     except LabellerrError as e:
    #         print(f"An error occurred: {e}")
    #         raise




    # def test_get_all_projects_by_client_id(self):
    #     """
    #     Test retrieving all datasets by a client's ID.
    #     """
    #     # Test configuration
    #     client_id = '1'  # Replace with a valid client ID
    #     try:
    #         result = self.client.get_all_project_per_client_id(client_id)
    #         self.assertTrue(result)  # Ensure the result is not empty
    #         self.assertTrue('response' in result)
    #         self.assertTrue(isinstance(result['response'], list))

    #         # print(result)
    #         print("Validation of get all Projects by client ID result: SUCCESS")

    #     except LabellerrError as e:
    #         print(f"An error occurred: {e}")
    #         raise




    # def test_link_dataset_to_project(self):
    #     """
    #     Test test linking project to a dataset.
    #     """
    #     # Test configuration
    #     client_id = '1'  # Replace with a valid client ID
    #     project_id = 'jeana_extensive_mouse_71114'  # Retrieve all datasets regardless of data type
    #     dataset_id='dataset-image-c4692c11-818f-40eb-82f5-c'
    #     try:
    #         result = self.client.link_dataset_to_project(client_id,project_id,dataset_id )
    #         self.assertTrue('success' in result or 'error' in result)
    #         if 'error' in result:
    #             self.assertTrue('code' in result['error'])
    #             self.assertTrue('msg' in result['error'])
    #             print(f"Project linking to a dataset is failed with following msg \"{result['error']['msg']}\" and {result['error']['code']} code")
    #         else:
    #             print("Project linking to a dataset is SUCCESS")

    #     except LabellerrError as e:
    #         print(f"An error occurred: {e}")
    #         raise

    # def test_get_all_dataset(self):
    #     """
    #     Test retrieving all datasets for a client.
    #     """
    #     try:
    #         # Test configuration
    #         client_id = '8482'
    #         data_type = 'image'
    #         scope='project'
    #         project_id='christye_complex_elephant_96597'

    #         # Call the method
    #         result = self.client.get_all_dataset(client_id, data_type,project_id,scope)

    #         # Verify the response structure
    #         self.assertIsInstance(result, dict)
    #         self.assertIn('linked', result)
    #         self.assertIn('unlinked', result)
    #         self.assertIsInstance(result['linked'], list)
    #         self.assertIsInstance(result['unlinked'], list)

    #         print(result['linked'])


    #         # Log success
    #         print(f"Successfully retrieved {len(result['linked'])} linked and {len(result['unlinked'])} unlinked datasets")

    #     except LabellerrError as e:
    #         print(f"An error occurred: {e}")
    #         raise

if __name__ == '__main__':
    unittest.main()
