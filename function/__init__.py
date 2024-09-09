
import os
import json
import azure.functions as func
from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication
from azure.devops.v7_1.git.models import GitVersionDescriptor
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.identity import ManagedIdentityCredential
from azure.devops.v7_1.build.models import Build, BuildDefinitionReference



#Crate a blob connection and return container connection 
def create_blob_connection(container_name, storage_account_name, mi_credential):

    print("Connecting to Azure Blob Storage using Managed Identity...")

    try:

        # Replace with your Blob Storage endpoint
        account_url = f"https://{storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential= mi_credential)

        # Create a container client
        container_client = blob_service_client.get_container_client(container_name)

        # Ensure the container exists
        if not container_client.exists():
            container_client.create_container()

        print("Returning container_client")

        return container_client

    except Exception as e:
        print(f"An error occurred while connecting to blob storage: {e}")
        raise


#Uplaod each file to continer 
def upload_file_to_blob(blob_client, file_content, filename):
    try:
        print(f"Uploading '{filename}'...")
        blob_client.upload_blob(file_content, overwrite=True)
        print(f"Successfully uploaded '{filename}' to container.")
    except Exception as e:
        print(f"Failed to upload '{filename}': {e}")
        raise



#Make conection to devops and start file transfer.
def transfer_files_from_devops_to_blob(pat, base_url, project, repository, branch_name, file_path, container_client, mi_credentials):
    
    # Wrap DefaultAzureCredential with BasicAuthentication for Azure DevOps client
    credentials = BasicAuthentication('', '')  # Required by the library, but we'll use `credential` directly in the API calls
    connection = Connection(base_url=base_url, creds= mi_credentials)

    try:
        git_client = connection.clients.get_git_client()

        items = git_client.get_items(
            repository_id=repository,
            project=project,
            scope_path=file_path,
            version_descriptor=GitVersionDescriptor(
                version=branch_name,
                version_type='branch'
            ),
            recursion_level='Full'
        )

        files_list = [item.path for item in items if item.git_object_type == 'blob' and (item.path.endswith('.pdf') or item.path.endswith('.csv'))]

        if not files_list:
            print("No PDF or CSV files found in the repository.")
            return

        for file in files_list:
            item_content = git_client.get_item_content(
                repository_id=repository,
                project=project,
                path=file,
                version_descriptor=GitVersionDescriptor(
                    version=branch_name,
                    version_type='branch'
                )
            )

            content = b''.join(item_content) if hasattr(item_content, '__iter__') else item_content

            # Prepare blob client
            blob_filename = os.path.basename(file)
            blob_client = container_client.get_blob_client(blob_filename)

            # Upload directly to blob storage
            upload_file_to_blob(blob_client, content, blob_filename)

    except Exception as e:
        print(f"An error occurred: {e}")
        raise



def main(req: func.HttpRequest) -> func.HttpResponse:

    organization = 'adarshs0791'
    project = 'FAQ_Copilot'
    repository = 'csv_files' 
    branch_name = 'main'  
    file_path = ''  
    
    # container_name = os.getenv('ContainerName')
    # storage_account_name = os.getenv('StorageAccountName')
    container_name = "empty-container"
    storage_account_name = "faqtestblobstorage"

    base_url = f'https://dev.azure.com/{organization}'

    
    # Initialize Managed Identity credential
    credential = DefaultAzureCredential()
    
    # Create Blob connection
    try:
        print("Connecting to Azure Blob Storage...")
        container_client = create_blob_connection(container_name, storage_account_name, credential)
    except Exception as e:
        print(f"An error occurred while connecting to Blob Storage: {e}")
        return func.HttpResponse(f"An error occurred while connecting to Blob Storage: {e}", status_code=500)


    # Start tranferring files
    try:
        print("Starting to transfer files...")
        transfer_files_from_devops_to_blob(base_url, project, repository, branch_name, file_path, container_client, credential)
        print("All files have been transfered successfully.")
    except Exception as e:
        print(f"An error occurred while transfering files: {e}")
        return func.HttpResponse(f"An error occurred while transfering files: {e}", status_code=500)

    #Return failures for main
    return func.HttpResponse("Process completed successfully.", status_code=200)



# --------------------------------------------------------------------------------------------------------------------------------------------------
# Final Code
#Get files (csv, pdf) from devops
#Upload it to blob directly.

# import os
# import json
# import azure.functions as func
# from azure.devops.connection import Connection
# from msrest.authentication import BasicAuthentication
# from azure.devops.v7_1.git.models import GitVersionDescriptor
# from azure.storage.blob import BlobServiceClient


# def create_blob_connection(container_name):

#     print("Connecting to Azure Blob Storage...")

#     try:
#         # Retrieve account name and key from environment variables
#         account_name = os.getenv('StorageAccountName')
#         account_key = os.getenv('StorageAccountKey')

#         # Construct the connection string
#         connection_string = f'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'

#         # Initialize BlobServiceClient
#         blob_service_client = BlobServiceClient.from_connection_string(connection_string)

#         # Create a container client
#         container_client = blob_service_client.get_container_client(container_name)

#         # Ensure the container exists
#         if not container_client.exists():
#             container_client.create_container()

#         print("Returning container_client")

#         return container_client

#     except Exception as e:
#         print(f"An error occurred while connecting to blob storage: {e}")
#         raise


# def upload_file_to_blob(blob_client, file_content, filename):
#     try:
#         print(f"Uploading '{filename}'...")
#         blob_client.upload_blob(file_content, overwrite=True)
#         print(f"Successfully uploaded '{filename}' to container.")
#     except Exception as e:
#         print(f"Failed to upload '{filename}': {e}")
#         raise


# def transfer_files_from_devops_to_blob(pat, base_url, project, repository, branch_name, file_path, container_client):

#     credentials = BasicAuthentication('', pat)
#     connection = Connection(base_url=base_url, creds=credentials)

#     try:
#         git_client = connection.clients.get_git_client()
#         items = git_client.get_items(
#             repository_id=repository,
#             project=project,
#             scope_path=file_path,
#             version_descriptor=GitVersionDescriptor(
#                 version=branch_name,
#                 version_type='branch'
#             ),
#             recursion_level='Full'
#         )

#         files_list = [item.path for item in items if item.git_object_type == 'blob' and (item.path.endswith('.pdf') or item.path.endswith('.csv'))]

#         if not files_list:
#             print("No PDF or CSV files found in the repository.")
#             return  # Just return; no need to send an HTTP response here

#         for file in files_list:
#             item_content = git_client.get_item_content(
#                 repository_id=repository,
#                 project=project,
#                 path=file,
#                 version_descriptor=GitVersionDescriptor(
#                     version=branch_name,
#                     version_type='branch'
#                 )
#             )

#             content = b''.join(item_content) if hasattr(item_content, '__iter__') else item_content

#             # Prepare blob client
#             blob_filename = os.path.basename(file)
#             blob_client = container_client.get_blob_client(blob_filename)

#             # Upload directly to blob storage
#             upload_file_to_blob(blob_client, content, blob_filename)
        
#     except Exception as e:
#         print(f"An error occurred: {e}")
#         raise


# def main(req: func.HttpRequest) -> func.HttpResponse:
#     organization = 'adarshs0791'
#     project = 'FAQ_Copilot'
#     repository = 'csv_files' 
#     pat = os.getenv('PAT')
#     branch_name = 'main'  
#     file_path = ''  
#     # container_name = 'devops-ingestion-container'
#     container_name = 'data-ingestion-poc-container'
#     base_url = f'https://dev.azure.com/{organization}'
    
#     container_client = create_blob_connection(container_name)

#     try:
#         print("Starting to transfer files...")
#         transfer_files_from_devops_to_blob(pat, base_url, project, repository, branch_name, file_path, container_client)
#         print("All files have been transfered successfully.")
#     except Exception as e:
#         print(f"An error occurred while transfering files: {e}")
#         return func.HttpResponse(f"An error occurred while transfering files: {e}", status_code=500)

#     return func.HttpResponse("Process completed successfully.", status_code=200)


# --------------------------------------------------------------------------------------------------------------------------------------------------
