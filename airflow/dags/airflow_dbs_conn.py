#python 3.9.8 64-bit

# from multiprocessing.dummy import DummyProcess
from airflow.models import DAG
from airflow.models.connection import Connection
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

c = Connection(
    conn_id = "mysql_conn",
    conn_type = "MySQL",
    description = "None",
    host = "localhost",
    port = "3306",
    login = "airflow",
    password = "airflow"
)

args = {
    'owner': 'maulfi_a',
    'start_date': days_ago(1),
    'email': ['alfansurimaulfi@gmail.com'],
    'email_on_failure': True,
    'retries': 10
}

with DAG(
    dag_id = 'chapter43_maulfia',
    description = 'Endgame',
    default_args=args,
    schedule_interval='15 5 29 1 *') as dag:

    dummy_task = DummyOperator(
        task_id = 'initial_task'
    )

    dbs_conn_task = BashOperator(
        task_id = 'dbs_connector',
        bash_command = f"""airflow connections add 'mysqldb_ch43' --conn-uri {c.get_uri()}"""
    )

    dummy_task >> dbs_conn_task
#if __name__ == __main__:
    