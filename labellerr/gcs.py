import os
import requests
CONTENT_TYPE = 'application/octet-stream'
def upload_to_gcs_direct(signed_url, file_path):
    with open(file_path, "rb") as f:
        file_data = f.read()

    headers = {
        "Content-Type": CONTENT_TYPE,
    }
    upload_response = requests.put(signed_url, headers=headers, data=file_data)

    if upload_response.status_code in (200, 201):
        return True
    else:
        raise AssertionError(f"Upload failed: {upload_response.status_code}, {upload_response.text}")
def upload_to_gcs_resumable(signed_url, file_path):

    # Step 1: Start a resumable upload session
    headers = {
        "x-goog-resumable": "start",
        "Content-Type": CONTENT_TYPE,
    }
    response = requests.post(signed_url, headers=headers)
    
    if response.status_code != 201:
        raise AssertionError(f"Failed to start resumable session: {response.status_code}, {response.text}")

    upload_url = response.headers["Location"]

    # Step 2: Upload the whole file in a single PUT request
    file_size = os.path.getsize(file_path)
    with open(file_path, "rb") as f:
        file_data = f.read()

    headers = {
        "Content-Type": CONTENT_TYPE,
        "Content-Range": f"bytes 0-{file_size-1}/{file_size}",
    }
    
    upload_response = requests.put(upload_url, headers=headers, data=file_data)

    if upload_response.status_code in (200, 201):
        return True
    else:
        raise AssertionError(f"Upload failed: {upload_response.status_code}, {upload_response.text}")
    