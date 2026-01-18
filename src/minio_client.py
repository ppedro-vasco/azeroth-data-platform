import boto3
import json
import os
from botocore.exceptions import ClientError

class MinIOClient:
    def __init__(self):
        self.endpoint_url = os.getenv("MINIO_ENDPOINT")
        self.access_key = os.getenv("MINIO_ROOT_USER")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD")
        self.bucket_name = "bronze"

        self.s3_client = boto3.client(
            's3',
            endpoint_url = self.endpoint_url,
            aws_access_key_id = self.access_key,
            aws_secret_access_key = self.secret_key
        )
    
    def _create_bucket_if_not_exists(self):
        try:
            self.s3_client.head_bucket(Bucket = self.bucket_name)
        except ClientError:
            print(f"Bucket ' {self.bucket_name}' não encontrado. Criando...")
            self.s3_client.create_bucket(Bucket=self.bucket_name)

    def save_json(self, data, filename):
        self._create_bucket_if_not_exists()

        json_bytes = json.dumps(data, indent=2).encode('utf-8')

        try:
            self.s3_client.put_object(
                Bucket = self.bucket_name,
                Key = filename,
                Body = json_bytes,
                ContentType = 'application/json'
            )
            return True
        except ClientError as e:
            raise Exception(f"Erro ao salvar no MinIO: {e}")