# Labellerr SDK Documentation: Simplified Guide  

## Table of Contents  

1. [What is the Labellerr SDK?](#what-is-the-labellerr-sdk)  
2. [How to Install](#how-to-install)  
3. [Getting Started](#getting-started)  
4. [Key Features](#key-features)  
   - [Creating a New Project](#creating-a-new-project)  
   - [Uploading Annotations](#uploading-annotations)  
   - [Exporting Project Data](#exporting-project-data)  
5. [Dealing with Errors](#dealing-with-errors)  
6. [Where to Get Help](#where-to-get-help)  

---

## What is the Labellerr SDK?  

The **Labellerr SDK** is a toolkit for Python that helps you work with the Labellerr platform easily. Whether you need to start a new project, upload annotations, or download data, this SDK simplifies the process for you.  

---

## How to Install  

To set up the Labellerr SDK, just open your terminal and run:  

```bash  
pip install https://github.com/tensormatics/SDKPython/releases/download/v1/labellerr_sdk-1.0.0.tar.gz  
```  

This will install everything you need to use the SDK in your Python programs.  

---

## Getting Started  

To start using the SDK:  

1. **Import the SDK**  
   Add the following line to your Python script:  

   ```python  
   from labellerr.client import LabellerrClient  
   ```  

2. **Log In with Your API Keys**  
   Replace `'your_api_key'` and `'your_api_secret'` with your actual credentials:  

   ```python  
   client = LabellerrClient('your_api_key', 'your_api_secret')  
   ```  

This sets up your connection to the Labellerr platform, making the SDK ready to use.  

---

## Key Features  

### Creating a New Project  

When you want to start a new project on Labellerr, here’s what to do:  

1. **Prepare the Project Details**  
   Describe your project using a dictionary. For example:  

   ```python  
   project_payload = {
       'client_id': '12345',  
       'dataset_name': 'Sample Dataset',  
       'dataset_description': 'Dataset for image classification',  
       'data_type': 'image',  
       'created_by': 'user@example.com',  
       'project_name': 'Image Classification Project',  
       'annotation_guide': [  
           {  
               "question_number": 1,  
               "question": "What is the main object in the image?",  
               "options": [{"option_name": "Car"}, {"option_name": "Building"}],  
               "option_type": "SingleSelect",  
               "required": True  
           }  
       ],  
       'folder_to_upload': '/path/to/images'  
   }  
   ```  

2. **Create the Project**  

   ```python  
   try:  
       result = client.initiate_create_project(project_payload)  
       print(f"Project created! Project ID: {result['project_id']}")  
   except LabellerrError as e:  
       print(f"Error creating project: {e}")  
   ```  

This creates the project and provides you with a unique Project ID.  

---

### Uploading Annotations  

If you have annotations ready, you can upload them directly to a project.  

1. **Get Your Annotation File Ready**  
   Ensure your file follows the correct format (e.g., COCO, YOLO).  

2. **Upload the Annotations**  

   ```python  
   try:  
       result = client.upload_preannotation_by_project_id(  
           project_id='project_123',  
           client_id='12345',  
           annotation_format='coco',  
           annotation_file='/path/to/annotations.json'  
       )  
       print("Annotations uploaded successfully!")  
   except LabellerrError as e:  
       print(f"Error uploading annotations: {e}")  
   ```  

This updates the project with your annotation data.  

---

### Exporting Project Data  

You can easily download project data to your local machine.  

1. **Set Up the Export Details**  

   ```python  
   export_config = {  
       "export_name": "Weekly Export",  
       "export_description": "Accepted annotations only",  
       "export_format": "json",  
       "export_destination": "local",  
       "statuses": ["accepted"]  
   }  
   ```  

2. **Download the Data**  

   ```python  
   try:  
       result = client.create_local_export('project_123', '12345', export_config)  
       print(f"Export successful! Export ID: {result['export_id']}")  
   except LabellerrError as e:  
       print(f"Error exporting data: {e}")  
   ```  

---

## Dealing with Errors  

Sometimes things don’t go as planned, but the SDK helps you identify what went wrong.  

1. Wrap your code in a `try-except` block to catch errors:  

   ```python  
   from labellerr.exceptions import LabellerrError  

   try:  
       # Example function call  
       result = client.some_function(args)  
   except LabellerrError as e:  
       print(f"An error occurred: {e}")  
   ```  

2. The SDK provides clear error messages to help you troubleshoot.  

---

## Where to Get Help  

If you need assistance:  

- **Email**: [support@labellerr.com](mailto:support@labellerr.com)  
- **Documentation**: [Labellerr Docs](https://docs.labellerr.com)  