from airflow.models.baseoperator import BaseOperator
from airflow.hooks.S3_hook import S3Hook
import re
import pickle

class MapOperator(BaseOperator):

    def __init__(self, hook: str, *args, **kwargs):
        self.hook = hook
        super().__init__(*args, **kwargs)

    def execute(self, context):
        s3 = S3Hook(self.hook)
        task_id = context['ti'].task_id
        file_key = context['ti'].xcom_pull(key=task_id, task_ids=['read_data'])
        ser_list = s3.read_key(file_key)
        tweets = pickle.loads(ser_list)[0]
        word_dict = dict()
        for tweet in tweets:
            for word in tweet.split():
                if re.match('^[a-z]+$', word):
                    word = word.lower()
                    word_dict[word] = word_dict.get(word, 0) + 1
        ser_dict = pickle.dumps(word_dict)
        s3.load_bytes(ser_dict, f'{task_id}_result')
        return f'{task_id}_result'
