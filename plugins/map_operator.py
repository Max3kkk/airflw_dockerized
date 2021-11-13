from airflow.models.baseoperator import BaseOperator
import re


class MapOperator(BaseOperator):

    def __init__(self, slice_index: int,  *args, **kwargs):
        self.slice_index = slice_index
        super().__init__(*args, **kwargs)

    def execute(self, context):
        tweets = context['ti'].xcom_pull(key=f'tweet_list_{self.slice_index}', task_ids=['read_data'])[0]
        word_dict = dict()
        for tweet in tweets:
            for word in tweet.split():
                if re.match('^[a-z]+$', word):
                    word = word.lower()
                    word_dict[word] = word_dict.get(word, 0) + 1
        return word_dict
