from azure.storage.blob import BlobServiceClient
from azure.storage.blob import ContainerClient


accName = "dori1328@hotmail.com"
accKey = "4ESZQHtkho2LvPKoopzXFLBEZVwC/6+i62XT56tkzVSPptC7rYJjXufWMrVat/DYjWxmf5kfza+6+AStPYmOZQ=="

connection_string="DefaultEndpointsProtocol=https;AccountName="+accName+";AccountKey="+accKey+";EndpointSuffix=core.windows.net"
container_name='test'

print("connection_string = " + connection_string)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

file_path='test/test01/test02/'
get_list = list()

for blob in container_client.list_blobs(name_starts_with=file_path):
    get_list.append(blob.name)

print(get_list)