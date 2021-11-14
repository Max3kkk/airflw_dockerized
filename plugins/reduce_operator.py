from airflow.models.baseoperator import BaseOperator
from airflow.hooks.S3_hook import S3Hook
import pickle


class ReduceOperator(BaseOperator):

    def __init__(self, hook: str, task_ids: list, *args, **kwargs):
        self.hook = hook
        self.task_ids = task_ids
        super().__init__(*args, **kwargs)

    def execute(self, context):
        s3 = S3Hook(self.hook)
        res_dict = dict()
        for task_id in self.task_ids:
            file_key = context['ti'].xcom_pull(task_id)
            ser_dict = s3.read_key(file_key)
            word_dicts = pickle.loads(ser_dict)
            for word_dict in word_dicts:
                for word in word_dict:
                    res_dict[word] = res_dict.get(word, 0) + word_dict[word]
        ser_dict = pickle.dumps(res_dict)
        s3.load_bytes(ser_dict, 'reduce_result')
        return f'reduce_result'
