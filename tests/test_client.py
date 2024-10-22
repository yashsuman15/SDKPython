import unittest
from labellerr.client import LabellerrClient
from labellerr.exceptions import LabellerrError
import json

class TestLabellerrClient(unittest.TestCase):
    def setUp(self):
        self.client = LabellerrClient('f483bd.53bac54d36b8382a176138d46c', 'c9d7255834ca6b67f55a6c5f60121d4fe7e47851c022bc0237fecda541ab0eae')

    def test_get_file(self):
        project_id = 'veronike_loose_raven_39117'
        client_id = '8748'
        email_id = 'angansen@gmail.com'
        uuid = '6ca9ff2c-a500-4125-8912-f74351ba9c9d'
        try:
            result = self.client.get_file(project_id, client_id, email_id, uuid)
            print(json.dumps(result, indent=4))  # Log the return value to the console in a pretty JSON format
        except LabellerrError as e:
            print(f"An error occurred: {e}")  # Log the error message

if __name__ == '__main__':
    unittest.main()

# python -m unittest discover -s tests