from minio import Minio
import os

class MyMinio:
    def __init__(self, endpoint: str, access_key: str, secret_key: str):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.minio_client = self.get_minio_client()

    def get_minio_client(self):
        return Minio(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False
        )

    def create_bucket(self, bucket_name: str):
        if not self.minio_client.bucket_exists(bucket_name):
            self.minio_client.make_bucket(bucket_name)

    def add_file_to_bucket(self, bucket_name: str):
        pass


