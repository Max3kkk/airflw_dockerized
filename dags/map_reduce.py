from datetime import datetime

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from map_operator import MapOperator
from reduce_operator import ReduceOperator
import os

# aws_conn_id=minio_connectioncd
MINIO_CONN_ID = 'local_minio'
POSTGRES_CONN_ID = 'postrgre_sql'
INPUT_PATH = 'input_data/tweets.csv'
OUTPUT_PATH = 'word_frequency.csv'
MAIN_BUCKET = 'datalake'
SPLIT_NUM = 10


def init_s3(filename: str = INPUT_PATH, key: str = 'tweets.csv', **context):
    # get s3 connection with minio via s3hook
    s3 = S3Hook(MINIO_CONN_ID)
    if not s3.check_for_bucket(MAIN_BUCKET):
        s3.create_bucket(MAIN_BUCKET)
    s3.load_file(filename=filename, key=key, bucket_name=MAIN_BUCKET, replace=True)
    return key


def read_data(**context):
    import pandas as pd
    import numpy as np
    import pickle
    import io

    task_instance = context['task_instance']
    s3 = S3Hook(MINIO_CONN_ID)

    key = task_instance.xcom_pull('init_s3')
    obj = s3.get_key(key=key, bucket_name=MAIN_BUCKET)
    main_df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))
    content = main_df['content'].tolist()
    splits = np.array_split(content, SPLIT_NUM)
    for i in range(SPLIT_NUM):
        split = splits[i].tolist()
        ser_list = pickle.dumps(split)
        s3.load_bytes(bytes_data=ser_list, key=f'tweet_list_{i}', bucket_name=MAIN_BUCKET, replace=True)
        task_instance.xcom_push(key=f'map_{i}', value=f'tweet_list_{i}')


def write_data(**context):
    import pandas as pd
    import pickle

    s3 = S3Hook(MINIO_CONN_ID)
    task_instance = context['task_instance']

    file_key = task_instance.xcom_pull('reduce')
    obj = s3.get_key(file_key, bucket_name=MAIN_BUCKET)
    ser_dict = obj.get()['Body'].read()
    res_dict = pickle.loads(ser_dict)
    df = pd.DataFrame(res_dict.items(), columns=['word', 'frequency'])
    sorted_df = df.sort_values(by=['frequency'], ascending=False)
    sorted_df.to_csv(OUTPUT_PATH)
    print(f'csv path {os.path.abspath(OUTPUT_PATH)}')
    s3.load_file(OUTPUT_PATH, 'word_frequency.csv', replace=True, bucket_name=MAIN_BUCKET)
    return 'word_frequency.csv'


def csv_to_db():
    # Open Postgres Connection
    pg_hook = PostgresHook(POSTGRES_CONN_ID)
    get_postgres_conn = pg_hook.get_conn()
    curr = get_postgres_conn.cursor()

    curr.execute("DROP TABLE IF EXISTS tweets")
    curr.execute("CREATE TABLE tweets(id SERIAL PRIMARY KEY, word VARCHAR(255) NOT NULL, frequency INT NOT NULL)")
    # CSV loading to table.
    with open(OUTPUT_PATH, 'r') as f:
        next(f)
        curr.copy_from(f, 'tweets', sep=',')
        get_postgres_conn.commit()


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
            conn_id=MINIO_CONN_ID,
            bucket_name=MAIN_BUCKET,
            task_id=f'map_{i}'
        ))
    reduce = ReduceOperator(
        task_ids=[task.task_id for task in map_tasks],
        conn_id=MINIO_CONN_ID,
        bucket_name=MAIN_BUCKET,
        task_id="reduce"
    )
    #
    # create_table = PostgresOperator(
    #     task_id='create_table',
    #     postgres_conn_id=POSTGRES_CONN_ID,
    #     sql='''
    #             DROP TABLE IF NOT EXISTS tweets;
    #             CREATE TABLE tweets (
    #             word VARCHAR(120) UNIQUE NOT NULL,
    #             frequency SERIAL NOT NULL);
    #         ''',
    # )

    csv_to_db = PythonOperator(
        task_id='csv_to_db',
        python_callable=csv_to_db
    )

    #
    # copy_csv = PostgresOperator(
    #     task_id='copy_csv',
    #     postgres_conn_id=POSTGRES_CONN_ID,
    #     sql='''
    #             COPY tweets
    #             FROM %(file_path)s
    #             DELIMITER ','
    #             CSV HEADER;
    #         ''',
    #     # parameters={"file_path": os.path.abspath(OUTPUT_PATH)},
    #     parameters={"file_path": OUTPUT_PATH},
    # )

    init_s3 >> read_data >> map_tasks >> reduce >> write_data >> csv_to_db
