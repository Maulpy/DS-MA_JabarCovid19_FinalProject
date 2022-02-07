#python 3.9.8 64-bit

# from multiprocessing.dummy import DummyProcess
from airflow.models import DAG
from airflow.models.connection import Connection
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'maulfi_a',
    'start_date': days_ago(1),
    'email': ['alfansurimaulfi@gmail.com'],
    'email_on_failure': True,
    'retries': 10
}

with DAG(
    dag_id = 'provincemonthly_postgreswriter',
    description = 'Endgame postgreswriter province monthly',
    default_args=args,
    schedule_interval='0 10 */1 * *') as dag:  #UTC+7 17:00 WIB Daily

    dummy_task = DummyOperator(
        task_id = 'initial_task'
    )

    provincedaily_writing_task = BashOperator(
        task_id = 'postgres_writer_monthly',
        bash_command = f"set -e; python /opt/airflow/dags/file/province_monthly.py"
    )

    dummy_task >> provincedaily_writing_task
#if __name__ == __main__: