from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "basic_dags",
    "email": ["dharana7723@gmail.com"],
    "email_on_failture": False
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

    t1 = DummyOperator(
        task_id="dummy_task_id",
        retries=5,
    )

    t2 = PythonOperator(
        task_id="test_airflow",
        python_callable=print_hello
    )

    t1 >> t2

    