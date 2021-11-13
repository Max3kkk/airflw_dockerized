from airflow.models.baseoperator import BaseOperator


class ReduceOperator(BaseOperator):

    def __init__(self, slice_num: int, *args, **kwargs):
        self.slice_num = slice_num
        super().__init__(*args, **kwargs)

    def execute(self, context):
        res_dict = dict()
        for slice in range(self.slice_num):
            word_dicts = context['ti'].xcom_pull(task_ids=[f'map_{slice}'])
            for word_dict in word_dicts:
                for word in word_dict:
                    res_dict[word] = res_dict.get(word, 0) + word_dict[word] 
        return res_dict
