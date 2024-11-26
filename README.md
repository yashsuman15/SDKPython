Certainly! Here's a more focused documentation for the Labellerr SDK, covering the three main functionalities you've specified:

# Labellerr SDK Documentation

## Table of Contents
1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Getting Started](#getting-started)
4. [Core Functionalities](#core-functionalities)
   - [Initiating Project Creation](#initiating-project-creation)
   - [Uploading Pre-annotations](#uploading-pre-annotations)
   - [Creating Local Exports](#creating-local-exports)
5. [Error Handling](#error-handling)
6. [Support](#support)

## Introduction

The Labellerr SDK is a Python library that simplifies interaction with the Labellerr platform. This documentation focuses on three key functionalities: project creation, pre-annotation upload, and local export creation.

## Installation

Install the Labellerr SDK using pip:

```bash
pip install https://github.com/tensormatics/SDKPython/releases/download/v1/labellerr_sdk-1.0.0.tar.gz
```

## Getting Started

To use the Labellerr SDK, first import and initialize the `LabellerrClient`:

```python
from labellerr.client import LabellerrClient

# Initialize the client with your API credentials
client = LabellerrClient('your_api_key', 'your_api_secret')
```

## Core Functionalities

### Initiating Project Creation

Create a new project with specified configurations.

```python
def initiate_create_project(self, payload):
    """
    Initiates the creation of a new project.

    Args:
        payload (dict): A dictionary containing project configuration details.

    Returns:
        dict: A dictionary containing the project creation response.

    Raises:
        LabellerrError: If project creation fails.
    """
```

Example usage:

```python
project_payload = {
    'client_id': '12345',
    'dataset_name': 'Sample Dataset',
    'dataset_description': 'A sample dataset for image classification',
    'data_type': 'image',
    'created_by': 'user@example.com',
    'project_name': 'Image Classification Project',
    'annotation_guide': [
        {
            "question_number": 1,
            "question": "What is the main object in the image?",
            "required": True,
            "options": [
                {"option_name": "Car"},
                {"option_name": "Building"},
                {"option_name": "Person"}
            ],
            "option_type": "SingleSelect"
        }
    ],
    'autolabel': False,
    'folder_to_upload': '/path/to/image/folder'
}

try:
    result = client.initiate_create_project(project_payload)
    print(f"Project created successfully. Project ID: {result['project_id']}")
except LabellerrError as e:
    print(f"Project creation failed: {str(e)}")
```

### Uploading Pre-annotations

Upload pre-annotations for a specific project.

```python
def upload_preannotation_by_project_id(self, project_id, client_id, annotation_format, annotation_file):
    """
    Uploads pre-annotations for a project.

    Args:
        project_id (str): The ID of the project.
        client_id (str): The ID of the client.
        annotation_format (str): The format of the annotations (e.g., 'coco', 'yolo').
        annotation_file (str): Path to the annotation file.

    Returns:
        dict: A dictionary containing the upload response.

    Raises:
        LabellerrError: If the upload fails.
    """
```

Example usage:

```python
project_id = 'project_123'
client_id = '12345'
annotation_format = 'coco'
annotation_file = '/path/to/annotations.json'

try:
    result = client.upload_preannotation_by_project_id(project_id, client_id, annotation_format, annotation_file)
    print("Pre-annotations uploaded successfully")
except LabellerrError as e:
    print(f"Pre-annotation upload failed: {str(e)}")
```

### Creating Local Exports

Create a local export of project data.

```python
def create_local_export(self, project_id, client_id, export_config):
    """
    Creates a local export of project data.

    Args:
        project_id (str): The ID of the project.
        client_id (str): The ID of the client.
        export_config (dict): Configuration for the export.

    Returns:
        dict: A dictionary containing the export response.

    Raises:
        LabellerrError: If the export creation fails.
    """
```

Example usage:

```python
project_id = 'project_123'
client_id = '12345'
export_config = {
    "export_name": "Weekly Export",
    "export_description": "Export of all accepted annotations",
    "export_format": "json",
    "export_destination": "local",
    "question_ids": ["all"],
    "statuses": ["accepted"]
}

try:
    result = client.create_local_export(project_id, client_id, export_config)
    print(f"Local export created successfully. Export ID: {result['export_id']}")
except LabellerrError as e:
    print(f"Local export creation failed: {str(e)}")
```

## Error Handling

The SDK uses custom `LabellerrError` exceptions. Always wrap your code in try-except blocks to handle these errors:

```python
from labellerr.exceptions import LabellerrError

try:
    # Your SDK function call here
    result = client.some_function(args)
except LabellerrError as e:
    print(f"An error occurred: {str(e)}")
```

## Support

For additional support or to report issues, please contact our support team at support@labellerr.com or visit our documentation website at https://docs.labellerr.com.