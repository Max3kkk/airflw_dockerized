from datetime import datetime

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
from map_operator import MapOperator
from reduce_operator import ReduceOperator

# aws_conn_id=minio_connection
MINIO_HOOK = 'minio_connection'
INPUT_PATH = 'input_data/tweets.csv'
OUTPUT_PATH = 'word_frequency.csv'
MAIN_BUCKET = 'main_bucket'
SPLIT_NUM = 10


def init_s3(filename: str = INPUT_PATH, key: str = 'tweets.csv', **context):
    # get s3 connection with minio via s3hook
    s3 = S3Hook(MINIO_HOOK)
    if not s3.get_bucket(MAIN_BUCKET):
        s3.create_bucket(MAIN_BUCKET)
    s3.load_file(filename=filename, key=key, bucket_name=MAIN_BUCKET, replace=True)
    return key


def read_data(**context):
    import pandas as pd
    import numpy as np
    import pickle

    task_instance = context['task_instance']
    s3 = S3Hook(MINIO_HOOK)

    key = task_instance.xcom_pull('init_s3')
    file = s3.read_key(key)
    main_df = pd.read_csv(file)
    content = main_df['content'].tolist()
    splits = np.array_split(content, SPLIT_NUM)
    for i in range(SPLIT_NUM):
        ser_list = pickle.dumps(splits[i].tolist())
        s3.load_bytes(ser_list, f'tweet_list_{i}')
        task_instance.xcom_push(key=f'map_{i}', value=f'tweet_list_{i}')


def write_data(**context):
    import pandas as pd
    import pickle

    s3 = S3Hook(MINIO_HOOK)
    task_instance = context['task_instance']

    file_key = task_instance.xcom_pull('reduce')
    ser_dict = s3.read_key(file_key)
    res_dict = pickle.loads(ser_dict)
    df = pd.DataFrame(res_dict.items(), columns=['word', 'frequency'])
    sorted_df = df.sort_values(by=['frequency'], ascending=False)
    sorted_df.to_csv(OUTPUT_PATH, index=False)

    s3.load_file(OUTPUT_PATH, 'word_frequency.csv')
    return 'word_frequency.csv'


default_args = {
    'start_date': datetime(2021, 1, 1)
}

with DAG(dag_id='map_reduce', schedule_interval='@once', default_args=default_args, catchup=False
         ) as dag:
    init_s3 = PythonOperator(
        task_id="init_s3",
        python_callable=init_s3
    )
    read_data = PythonOperator(
        task_id="read_data",
        python_callable=read_data
    )
    write_data = PythonOperator(
        task_id="write_data",
        python_callable=write_data
    )
    map_tasks = []
    for i in range(SPLIT_NUM):
        map_tasks.append(MapOperator(
            hook=MINIO_HOOK,
            task_id=f'map_{i}'
        ))
    reduce = ReduceOperator(
            task_ids=[task.task_id for task in map_tasks],
            hook=MINIO_HOOK,
            task_id="reduce"
        )
    init_s3 >> read_data >> map_tasks >> reduce >> write_data
