from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import logging

source_account_name = Variable.get("source_account_name")
source_account_key = Variable.get("source_account_key")
destination_account_name = Variable.get("destination_account_name")
destination_account_key = Variable.get("destination_account_key") 

# Azure Blob Storage 연결 설정
source_connection_string = 'DefaultEndpointsProtocol=https;AccountName={source_account_name};AccountKey={source_account_key};EndpointSuffix=core.windows.net'
source_container_name = 'test'
destination_connection_string = 'DefaultEndpointsProtocol=https;AccountName={destination_account_name};AccountKey={destination_account_key};EndpointSuffix=core.windows.net'
destination_container_name = 'test2'

print("source_connection_string=", source_connection_string)
print("destination_connection_string=", destination_connection_string)

def list_blobs_in_container(connection_string, container_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs()
    return [blob.name for blob in blob_list]

def download_blob(connection_string, container_name, blob_name, download_file_path):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    with open(download_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

def upload_blob(connection_string, container_name, blob_name, upload_file_path):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    with open(upload_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

def etl_task(**kwargs):
    # 소스 컨테이너에서 블롭 목록 가져오기
    source_blobs = list_blobs_in_container(source_connection_string, source_container_name)
    
    for blob_name in source_blobs:
        # 블롭 다운로드
        download_file_path = f"/tmp/{blob_name}"
        download_blob(source_connection_string, source_container_name, blob_name, download_file_path)
        
        # ETL 로직 추가
        # 이 부분에 필요한 데이터를 변환하는 로직을 추가할 수 있습니다.
        
        # 변환된 파일 업로드
        upload_blob(destination_connection_string, destination_container_name, blob_name, download_file_path)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'azure_blob_etl',
    default_args=default_args,
    description='An ETL process from one Azure Blob Storage to another',
    schedule_interval='@daily',
)

etl_task = PythonOperator(
    task_id='etl_task',
    python_callable=etl_task,
    provide_context=True,
    dag=dag,
)

etl_task

