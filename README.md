
## Table of Contents
1. [Introduction](#introduction)  
2. [Installation](#installation)  
3. [Data Types and Supported Formats](data-types-and-supported-formats)
4. [Getting Started](#getting-started)  
5. [Key Features](#key-features)  
   - [Creating a New Project](#creating-a-new-project)  
   - [Uploading Pre-annotations](#uploading-pre-annotations)  
   - [Exporting Project Data Locally](#exporting-project-data-locally)  
   - [Retrieving All Projects for a Client](#retrieving-all-projects-for-a-client)
   - [Retrieving All Datasets](#retrieving-all-datasets)
6. [Error Handling](#error-handling)  
7. [Support](#support)  

---

## Introduction

The **Labellerr SDK** is a Python library designed to make interacting with the Labellerr platform simple and efficient. With this SDK, you can manage data annotations, projects, and exports seamlessly in your applications.  

This documentation will guide you through installing the SDK, understanding its core functionalities, and handling common errors.  

---

## Installation

To install the Labellerr SDK, use the following command:

```bash
pip install https://github.com/tensormatics/SDKPython/releases/download/v1/labellerr_sdk-1.0.0.tar.gz
```

---


## Data Types and Supported Formats

The Labellerr SDK supports various data types and file formats for your annotation projects:

### Supported Data Types and Extensions

| Data Type | Description | Supported Extensions |
|-----------|-------------|---------------------|
| `image`   | Image files for visual annotation | `.jpg`, `.jpeg`, `.png`, `.bmp`, `.tiff` |
| `video`   | Video content for temporal annotation | `.mp4` |
| `audio`   | Audio files for sound annotation | `.mp3`, `.wav` |
| `document`| Document files for text analysis | `.pdf` |
| `text`    | Plain text files for text annotation | `.txt` |

When using the SDK methods, specify the data type as one of: `'image'`, `'video'`, `'audio'`, `'document'`, or `'text'`.

#### Example Usage with Data Types:

```python
# When creating a dataset
payload = {
    'client_id': '12345',
    'dataset_name': 'Image Dataset',
    'data_type': 'image',  # Specify the data type here
    # ... other configuration options
}

# When retrieving datasets
result = client.get_all_dataset(client_id='12345', data_type='image')
```

**Note**: Ensure your files match the supported extensions for the specified data type to avoid upload errors.

---


## Getting Started

Once installed, you can start by importing and initializing the `LabellerrClient`. This client will handle all communication with the Labellerr platform.  

### Example:

```python
from labellerr.client import LabellerrClient

# Initialize the client with your API credentials
client = LabellerrClient('your_api_key', 'your_api_secret')
```

Replace `'your_api_key'` and `'your_api_secret'` with your actual API credentials provided by Labellerr.  

---

## Key Features

### Creating a New Project

A **project** in Labellerr organizes your datasets and their annotations. Use the following method to create a project:

#### Limitations:
- Maximum of **2,500 files** per folder.
- Total folder size should not exceed **2.5 GB**.

#### Method:

```python
def initiate_create_project(self, payload):
    """
    Initiates the creation of a new project.
    Args:
        payload (dict): Contains project configurations.
    Returns:
        dict: Contains project details.
    """
```

#### Example Usage:

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
    'rotation_config': {
        'annotation_rotation_count': 0,
        'review_rotation_count': 1,
        'client_review_rotation_count': 0
    },
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

Pre-annotations help predefine labels for your dataset, speeding up the annotation process.  

#### Method:

```python
def upload_preannotation_by_project_id(self, project_id, client_id, annotation_format, annotation_file):
    """
    Uploads pre-annotations for a project.
    Args:
        project_id (str): The ID of the project.
        client_id (str): The ID of the client.
        annotation_format (str): Format of annotations (e.g., 'coco', 'yolo').
        annotation_file (str): Path to the annotation file.
    Returns:
        dict: Response containing the upload status.
    """
```

#### Example Usage:

```python
project_id = 'project_123'
client_id = '12345'
annotation_format = 'coco'
annotation_file = '/path/to/annotations.json'

try:
    result = client.upload_preannotation_by_project_id(project_id, client_id, annotation_format, annotation_file)
    print("Pre-annotations uploaded successfully.")
except LabellerrError as e:
    print(f"Pre-annotation upload failed: {str(e)}")
```

### Exporting Project Data Locally

Export project data to analyze, store, or share it with others.  

#### Acceptable Statuses:
- `r_skipped`
- `cr_skipped`
- `p_annotation`
- `review`
- `r_assigned`
- `rejected`
- `p_review`
- `client_review`
- `cr_assigned`
- `client_rejected`
- `critical`
- `accepted`
#### Method:

```python
def create_local_export(self, project_id, client_id, export_config):
    """
    Creates a local export of project data.
    Args:
        project_id (str): The ID of the project.
        client_id (str): The ID of the client.
        export_config (dict): Configuration for the export.
    Returns:
        dict: Contains export details.
    """
```

#### Example Usage:

```python
project_id = 'project_123'
client_id = '12345'
export_config = {
    "export_name": "Weekly Export",
    "export_description": "Export of all accepted annotations",
    "export_format": "json",
    "statuses": ["accepted"]
}

try:
    result = client.create_local_export(project_id, client_id, export_config)
    print(f"Local export created successfully. Export ID: {result['export_id']}")
except LabellerrError as e:
    print(f"Local export creation failed: {str(e)}")
```

---


### Retrieving All Projects for a Client

You can retrieve all projects associated with a specific client ID using the following method:

#### Method:

```python
def get_all_project_per_client_id(self, client_id):
    """
    Retrieves all projects associated with a client ID.
    Args:
        client_id (str): The ID of the client.
    Returns:
        dict: Contains a list of projects in the 'response' field.
    """
```

#### Example Usage:

```python
client_id = '12345'

try:
    result = client.get_all_project_per_client_id(client_id)
    
    # Check if projects were retrieved successfully
    if result and 'response' in result:
        projects = result['response']
        print(f"Found {len(projects)} projects:")
        for project in projects:
            print(f"- Project ID: {project.get('project_id')}")
            print(f"  Name: {project.get('project_name')}")
            print(f"  Type: {project.get('data_type')}")
except LabellerrError as e:
    print(f"Failed to retrieve projects: {str(e)}")
```

This method is useful when you need to:
- List all projects for a client
- Find specific project IDs
- Check project statuses
- Get an overview of client's work

The response includes detailed information about each project, including its ID, name, data type, and other relevant metadata.


---

### Retrieving All Datasets

You can retrieve both linked and unlinked datasets associated with a client using the following method:

#### Method:

```python
def get_all_dataset(self, client_id, data_type):
    """
    Retrieves all datasets (both linked and unlinked) for a client.
    Args:
        client_id (str): The ID of the client.
        data_type (str): Type of data (e.g., 'image', 'text', etc.)
    Returns:
        dict: Contains two lists - 'linked' and 'unlinked' datasets
    """
```

#### Example Usage:

```python
client_id = '12345'
data_type = 'image'

try:
    result = client.get_all_dataset(client_id, data_type)
    
    # Process linked datasets
    linked_datasets = result['linked']
    print(f"Found {len(linked_datasets)} linked datasets:")
    for dataset in linked_datasets:
        print(f"- Dataset ID: {dataset.get('dataset_id')}")
        print(f"  Name: {dataset.get('dataset_name')}")
        print(f"  Description: {dataset.get('dataset_description')}")

    # Process unlinked datasets
    unlinked_datasets = result['unlinked']
    print(f"\nFound {len(unlinked_datasets)} unlinked datasets:")
    for dataset in unlinked_datasets:
        print(f"- Dataset ID: {dataset.get('dataset_id')}")
        print(f"  Name: {dataset.get('dataset_name')}")
        print(f"  Description: {dataset.get('dataset_description')}")

except LabellerrError as e:
    print(f"Failed to retrieve datasets: {str(e)}")
```

This method is useful when you need to:
- Get an overview of all available datasets
- Find datasets that are linked to projects
- Identify unlinked datasets that can be associated with new projects
- Manage dataset organization and project associations

The response includes two lists:
- `linked`: Datasets that are already associated with projects
- `unlinked`: Datasets that are not yet associated with any project

Each dataset object contains detailed information including its ID, name, description, and other metadata.

---

## Error Handling

The Labellerr SDK uses a custom exception class, `LabellerrError`, to indicate issues during API interactions. Always wrap your function calls in `try-except` blocks to gracefully handle errors.  

#### Example:

```python
from labellerr.exceptions import LabellerrError

try:
    # Example function call
    result = client.initiate_create_project(payload)
except LabellerrError as e:
    print(f"An error occurred: {str(e)}")
```

---

## Support

If you encounter issues or have questions, feel free to contact the Labellerr support team:  

- **Email**: support@labellerr.com  
- **Documentation**: [Labellerr Documentation](https://docs.labellerr.com)  
