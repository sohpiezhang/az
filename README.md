# az
# commit

 git config --global credential.helper wincred
 git config --global user.name "sophie.zhang"
 git config --global user.email sophie.zhang.au@gmail.com

1 install wsl   after windows update assisstant
2 install az cli on wsl
3 az ad sp create-for-rbac --name SPdemo1
4 create rg  sa(storage account) container , upload csv
5 create adf to copy csv to output folder
6 set adf to github
7 create adf from json, or clone adf
https://docs.microsoft.com/en-us/azure/data-factory/copy-clone-data-factory
https://docs.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-azure-cli


 git config --global credential.helper wincred

 git config --global user.name "sophie zhang"
 git config --global user.email sophie.zhang.au@gmail.com



#!/usr/bin/env python
# coding: utf-8

# In[1]:


from azure.identity import ClientSecretCredential 
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import time
import os
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
import os, random
import os
import json
from datetime import datetime
# Import the needed management objects from the libraries. The azure.common library
# is installed automatically with the other libraries.
from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobClient


# In[2]:


def print_item(group):
    """Print an Azure object instance."""
    print("\tName: {}".format(group.name))
    print("\tId: {}".format(group.id))
    if hasattr(group, 'location'):
        print("\tLocation: {}".format(group.location))
    if hasattr(group, 'tags'):
        print("\tTags: {}".format(group.tags))
    if hasattr(group, 'properties'):
        print_properties(group.properties)

def print_properties(props):
    """Print a ResourceGroup properties instance."""
    if props and hasattr(props, 'provisioning_state') and props.provisioning_state:
        print("\tProperties:")
        print("\t\tProvisioning State: {}".format(props.provisioning_state))
    print("\n\n")

def print_activity_run_details(activity_run):
    """Print activity run details."""
    print("\n\tActivity run details\n")
    print("\tActivity run status: {}".format(activity_run.status))
    if activity_run.status == 'Succeeded':
        print("\tNumber of bytes read: {}".format(activity_run.output['dataRead']))
        print("\tNumber of bytes written: {}".format(activity_run.output['dataWritten']))
        print("\tCopy duration: {}".format(activity_run.output['copyDuration']))
    else:
        print("\tErrors: {}".format(activity_run.error['message']))


# In[5]:



AZURE_TENANT_ID="aebd532b-ffab-4aca-9a23-620370090cc8"
AZURE_CLIENT_ID="e1cace18-50c5-4d55-9c13-fa2c1543c06b"
AZURE_CLIENT_SECRET= "r1ryF2.3tDnn02VrkAV~YY-~bPgNi8O1~x"
AZURE_SUBSCRIPTION_ID="3600cd0c-0725-4a38-8ac6-e135b659c5c8"

subscription_id =     AZURE_SUBSCRIPTION_ID

Secretcredentials = ClientSecretCredential(
    client_id=AZURE_CLIENT_ID, 
    client_secret=AZURE_CLIENT_SECRET, 
    tenant_id=AZURE_TENANT_ID) 

client = ResourceManagementClient(Secretcredentials, subscription_id)
for item in client.resource_groups.list():  # under subscription
    print_item(item)


LOCATION = "australiaeast"
resource_group_params = {'location':'australiaeast'}
GROUP_NAME = "azure-sample-group"

# # Create Resource group
# print("Create Resource Group")
# print_item(
#     client.resource_groups.create_or_update(
#         GROUP_NAME, resource_group_params)
# )

# # Modify the Resource group
# print("Modify Resource Group")
# resource_group_params.update(tags={"hello": "world"})
# print_item(
#     client.resource_groups.create_or_update(
#         GROUP_NAME, resource_group_params)
# )
    


# In[8]:


storage_client = StorageManagementClient(Secretcredentials, subscription_id)

STORAGE_ACCOUNT_NAME = 'blobtest4sophie2'
GROUP_NAME = 'arm-test'

poller = storage_client.storage_accounts.begin_create(GROUP_NAME, STORAGE_ACCOUNT_NAME,
    {
        "location" : LOCATION,
        "kind": "StorageV2",
        "sku": {"name": "Standard_LRS"}
    }
)

# Long-running operations return a poller object; calling poller.result()
# waits for completion.
account_result = poller.result()
print(f"Provisioned storage account {account_result.name}")


# Step 3: Retrieve the account's primary access key and generate a connection string.
keys = storage_client.storage_accounts.list_keys(GROUP_NAME, STORAGE_ACCOUNT_NAME)

print(f"Primary key for storage account: {keys.keys[0].value}")

conn_string = f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={keys.keys[0].value}"

print(f"Connection string: {conn_string}")

# Step 4: Provision the blob container in the account (this call is synchronous)
CONTAINER_NAME = "blob-container-01"
container = storage_client.blob_containers.create(GROUP_NAME, STORAGE_ACCOUNT_NAME, CONTAINER_NAME, {})

# The fourth argument is a required BlobContainer object, but because we don't need any
# special values there, so we just pass empty JSON.

print(f"Provisioned blob container {container.name}")


# In[9]:


from azure.storage.blob import BlobClient
import pandas as pd

# Define parameters
connectionString = conn_string
containerName = CONTAINER_NAME
outputBlobName= "iris_setosa.csv"

# Establish connection with the blob storage account
blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)
filename = r'C:\Users\azhang\Downloads\batch-adf-pipeline-tutorial-master\iris.csv'
# Load iris dataset from the task node
df = pd.read_csv(filename)

# Take a subset of the records
df = df[df['Species'] == "setosa"]

# Save the subset of the iris dataframe locally in task node
df.to_csv(outputBlobName, index = False)

with open(outputBlobName, "rb") as data:
    blob.upload_blob(data)


# In[ ]:





# In[11]:



Secretcredentials = ClientSecretCredential(client_id=AZURE_CLIENT_ID, client_secret=AZURE_CLIENT_SECRET, tenant_id=AZURE_TENANT_ID) 

resource_client = ResourceManagementClient(Secretcredentials, subscription_id)
adf_client = DataFactoryManagementClient(Secretcredentials, subscription_id)


# In[12]:


df_resource = Factory(location='australiaeast')
df_name = 'rg-test-adf-test1'

rg_name = GROUP_NAME
df = adf_client.factories.create_or_update(rg_name, df_name, df_resource)
print_item(df)
while df.provisioning_state != 'Succeeded':
    df = adf_client.factories.get(rg_name, df_name)
    time.sleep(1)


# In[23]:


# Create an Azure Storage linked service
ls_name = 'storageLinkedService001'

# IMPORTANT: specify the name and key of your Azure Storage account.
storage_string = SecureString(value=conn_string)
ls_azure_storage = LinkedServiceResource(properties=AzureStorageLinkedService(connection_string=storage_string)) 
ls = adf_client.linked_services.create_or_update(rg_name, df_name, ls_name, ls_azure_storage)
print_item(ls)


# In[24]:


# Create an Azure blob dataset (input)
ds_name = 'ds_in'
ds_ls = LinkedServiceReference(reference_name=ls_name)
blob_path = 'blob-container-01'
blob_filename = 'iris_setosa.csv'
ds_azure_blob = DatasetResource(properties=AzureBlobDataset(
    linked_service_name=ds_ls, folder_path=blob_path, file_name=blob_filename))
ds = adf_client.datasets.create_or_update(
    rg_name, df_name, ds_name, ds_azure_blob)
print_item(ds)


# In[25]:


# Create an Azure blob dataset (output)
dsOut_name = 'ds_out'
output_blobpath = 'blob-container-01/output'
dsOut_azure_blob = DatasetResource(properties=AzureBlobDataset(linked_service_name=ds_ls, folder_path=output_blobpath))
dsOut = adf_client.datasets.create_or_update(
    rg_name, df_name, dsOut_name, dsOut_azure_blob)
print_item(dsOut)


# In[26]:


# Create a copy activity
act_name = 'copyBlobtoBlob'
blob_source = BlobSource()
blob_sink = BlobSink()
dsin_ref = DatasetReference(reference_name=ds_name)
dsOut_ref = DatasetReference(reference_name=dsOut_name)
copy_activity = CopyActivity(name=act_name, inputs=[dsin_ref], outputs=[
                             dsOut_ref], source=blob_source, sink=blob_sink)


# In[27]:


# Create a pipeline with the copy activity
p_name = 'copyPipeline'
params_for_pipeline = {}
p_obj = PipelineResource(
    activities=[copy_activity], parameters=params_for_pipeline)
p = adf_client.pipelines.create_or_update(rg_name, df_name, p_name, p_obj)
print_item(p)


# In[28]:



# Create a pipeline run
run_response = adf_client.pipelines.create_run(rg_name, df_name, p_name, parameters={})

# Monitor the pipeline run
time.sleep(30)
pipeline_run = adf_client.pipeline_runs.get(
    rg_name, df_name, run_response.run_id)
print("\n\tPipeline run status: {}".format(pipeline_run.status))
filter_params = RunFilterParameters(
    last_updated_after=datetime.now() - timedelta(1), last_updated_before=datetime.now() + timedelta(1))
query_response = adf_client.activity_runs.query_by_pipeline_run(
    rg_name, df_name, pipeline_run.run_id, filter_params)
print_activity_run_details(query_response.value[0])

# or run by batch or event trigered


# In[ ]:





# In[35]:


# # Delete Resource group and everything in it
# print("Delete Resource Group")
# delete_async_operation = client.resource_groups.delete(GROUP_NAME)
# delete_async_operation.wait()
# print("\nDeleted: {}".format(GROUP_NAME))


# In[ ]:





# In[ ]:





