import datetime

import pendulum

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

with DAG(
    dag_id='truata_de',
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['truata_de'],
) as dag:
    task_1 = DummyOperator(
        task_id='task_1',
    )

    task_2 = DummyOperator(
        task_id='task_2',
    )

    task_3 = DummyOperator(
        task_id='task_3',
    )

    task_3a = DummyOperator(
        task_id='wait_for_upstream',
    )

    task_4 = DummyOperator(
        task_id='task_4',
    )

    task_5 = DummyOperator(
        task_id='task_5',
    )

    task_6 = DummyOperator(
        task_id='task_6',
    )

    task_1 >> [task_2, task_3] >> task_3a >> [task_4, task_5, task_6]
