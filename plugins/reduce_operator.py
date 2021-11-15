import pickle

from airflow.hooks.S3_hook import S3Hook
from airflow.models.baseoperator import BaseOperator


class ReduceOperator(BaseOperator):

    def __init__(self, conn_id: str, bucket_name: str, task_ids: list, *args, **kwargs):
        self.conn_id = conn_id
        self.bucket_name = bucket_name
        self.task_ids = task_ids
        super().__init__(*args, **kwargs)

    def execute(self, context):
        s3 = S3Hook(self.conn_id)
        res_dict = dict()
        for task_id in self.task_ids:
            key = context['ti'].xcom_pull(task_id)
            obj = s3.get_key(key=key, bucket_name=self.bucket_name)
            ser_dict = obj.get()['Body'].read()
            word_dict = pickle.loads(ser_dict)
            print(word_dict)
            for word in word_dict:
                res_dict[word] = res_dict.get(word, 0) + word_dict[word]
        ser_dict = pickle.dumps(res_dict)
        s3.load_bytes(ser_dict, 'reduce_result', replace=True, bucket_name=self.bucket_name)
        return f'reduce_result'
