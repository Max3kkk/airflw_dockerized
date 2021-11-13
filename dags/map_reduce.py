
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime
from map_operator import MapOperator
from reduce_operator import ReduceOperator


MINIO_HOOK = 'minio_connection'
INPUT_PATH = 'tweets.csv'
OUTPUT_PATH = 'word_frequency.csv'
SPLIT_NUM = 10

def init_s3(**context):
    s3 = S3Hook(MINIO_HOOK)


def read_data(**context):
    import pandas as pd
    import numpy as np

    task_instance = context['task_instance']
    main_df = pd.read_csv(INPUT_PATH)
    content = main_df['content'].tolist()
    splits = np.array_split(content, SPLIT_NUM)
    for i in range(SPLIT_NUM):
        task_instance.xcom_push(
            key=f'tweet_list_{i}', value=splits[i].tolist())


def write_data(**context):
    import pandas as pd
    task_instance = context['task_instance']
    res_dict = task_instance.xcom_pull('reduce')
    df = pd.DataFrame(res_dict.items(), columns=['word', 'frequency'])
    sorted_df = df.sort_values(by=['frequency'], ascending=False)
    sorted_df.to_csv(OUTPUT_PATH, index=False)


default_args = {
    'start_date': datetime(2021, 1, 1)
}

with DAG(dag_id='map_reduce', schedule_interval='@once', default_args=default_args, catchup=False
         ) as dag:
    read_data = PythonOperator(
        task_id="read_data",
        python_callable=read_data
    )

    init_s3 = PythonOperator(
        task_id="init_s3",
        python_callable=init_s3
    )

    write_data = PythonOperator(
        task_id="write_data",
        python_callable=write_data,
    )

    reduce = ReduceOperator(
        # word_dicts="{{ task_instance.xcom_pull(task_ids=[f'map_{i}' for i in range(SPLIT_NUM)]) }}",
        slice_num=SPLIT_NUM,
        task_id="reduce"
    )

    map_tasks = []
    for i in range(SPLIT_NUM):
        map_tasks.append(MapOperator(
            # tweets= "{{ task_instance.xcom_pull(key=f'tweet_list_{i}', task_ids=['read_data']) }}",
            slice_index=i,
            task_id=f'map_{i}'
        ))
        read_data >> map_tasks >> reduce >> write_data
