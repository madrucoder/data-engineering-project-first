# SALVANDO EM CSV

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import boto3
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

TABLES = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'venda']


@dag(
    dag_id='postgres_to_minio_csv',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)
def postgres_to_minio_etl():

    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
        aws_secret_access_key=os.environ['MINIO_SECRET_KEY']
    )
    bucket_name = 'landing'

    @task
    def get_max_primary_key(table_name: str):
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=f"{table_name}/max_id.txt")
            return int(response['Body'].read().decode('utf-8'))
        except s3_client.exceptions.NoSuchKey:
            return 0

    @task
    def load_incremental_data(table_name: str, max_id: int):
        hook = PostgresHook(postgres_conn_id='postgres')
        conn = hook.get_conn()
        cursor = conn.cursor()

        primary_key = f'id_{table_name}'

        cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (table_name,)
        )
        # já confirmamos que row[0] funciona
        columns = [row[0] for row in cursor.fetchall()]
        columns_str = ", ".join(columns)

        cursor.execute(
            f"SELECT {columns_str} FROM {table_name} WHERE {primary_key} > %s",
            (max_id,)
        )
        rows = cursor.fetchall()

        if rows:
            df = pd.DataFrame(rows, columns=columns)
            csv_buffer = df.to_csv(index=False)

            s3_client.put_object(
                Bucket=bucket_name,
                Key=f"{table_name}/data_{max_id + 1}.csv",
                Body=csv_buffer
            )

            new_max = df[primary_key].max()

            s3_client.put_object(
                Bucket=bucket_name,
                Key=f"{table_name}/max_id.txt",
                Body=str(new_max)
            )

    # ------------- AQUI É A MÁGICA -------------
    # Nada de chamar XComArg como função
    # Nada de loop errado do Airflow
    # Agora funciona 100%
    for t in TABLES:
        max_id_task = get_max_primary_key.override(task_id=f"get_max_id_{t}")(t)
        load_incremental_data.override(task_id=f"load_data_{t}")(t, max_id_task)

dag = postgres_to_minio_etl()