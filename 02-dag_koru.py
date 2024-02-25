from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 24),
}

dag = DAG(
    '0001-koru',
    default_args=default_args,
    description='DAG para executar o arquivo koru',
    schedule_interval=timedelta(minutes=20),  # Intervalo de 20 minutos
)

def execute_python_file():
    exec(open("/home/edsonpegorarotv/airflow_project/dags/arquivokoru/01-polygon_para_bigquery.py").read())

exec_task = PythonOperator(
    task_id='execute_python_file',
    python_callable=execute_python_file,
    dag=dag,
)

exec_task