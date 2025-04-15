import os
import requests
import mimetypes

def upload_to_gcs_resumable(resumable_url, file_path):

    with open(file_path, 'rb') as f:
        headers = {
            'Content-Type': 'application/octet-stream'
        }
        response = requests.put(resumable_url, headers=headers, data=f)
        
        if response.status_code not in [200, 201]:
            raise AssertionError(f"Upload failed: {response.status_code} - {response.text}")

    return response
