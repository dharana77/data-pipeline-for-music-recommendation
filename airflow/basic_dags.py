from datetime import datetime

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

kst = pendulum.timezone("Asia/Seoul")

POSTGRES_CONN_ID = "postgres_default"

default_args = {
    "owner": "basic_dags",
    "email": ["dharana7723@gmail.com"],
    "email_on_failure": False
}

with DAG(
    dag_id="ex_hello_world",
    default_args=default_args,
    start_date=datetime(2022, 6, 21, tzinfo=kst),
    description="Basic DAG test",
    schedule_interval='@once',
    tags=["test"]
) as dag:

    def print_hello():
        print("Welcome to airflow test")

    def get_data_from_postgres():
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM test_table")
        result = cursor.fetchall()
        print(result)
        cursor.close()
        connection.close()

    t1 = DummyOperator(
        task_id="dummy_task_id",
        retries=5,
    )

    t2 = PythonOperator(
        task_id="test_airflow",
        python_callable=print_hello
    )

    t3 = PythonOperator(
        task_id="get_data_from_postgres",
        python_callable=get_data_from_postgres,
        op_kwargs={
            "copy_sql": "COPY(SELECT * FROM CUSTOMER \
            WHERE first_name ='john' ) TO \
            STDOUT WITH CSV HEADER"
        }
    )

    t1 >> t2
    t3

