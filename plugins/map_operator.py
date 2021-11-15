from airflow.models.baseoperator import BaseOperator
from airflow.hooks.S3_hook import S3Hook
import re
import pickle


class MapOperator(BaseOperator):

    def __init__(self, conn_id: str, bucket_name: str, *args, **kwargs):
        self.conn_id = conn_id
        self.bucket_name = bucket_name
        super().__init__(*args, **kwargs)

    def execute(self, context):
        s3 = S3Hook(self.conn_id)
        task_id = context['ti'].task_id
        key = context['ti'].xcom_pull(key=task_id, task_ids='read_data')
        obj = s3.get_key(key=key, bucket_name=self.bucket_name)
        ser_list = obj.get()['Body'].read()
        tweets = pickle.loads(ser_list)
        word_dict = dict()
        for tweet in tweets:
            for word in tweet.split():
                if re.match('^[a-z]+$', word):
                    word = word.lower()
                    word_dict[word] = word_dict.get(word, 0) + 1
        ser_dict = pickle.dumps(word_dict)
        s3.load_bytes(ser_dict, f'{task_id}_result', replace=True, bucket_name=self.bucket_name)
        return f'{task_id}_result'
