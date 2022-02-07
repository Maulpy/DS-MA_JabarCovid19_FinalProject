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

# file_path = "c:/Users/ALFANSURI/Digital Skola/Final_Project/executor_scripts/experiment.py"
# file_pathv2 = r"C:\Users\ALFANSURI\Digital_Skola\Final_Project\executor_scripts\experiment.ipynb" #convert to raw string with 'r'
# file_pathv3 = r"/mnt/d/Progress Files/Non-Time Progress/Endgame/Experiment/airflow/dags/experiment.py"
# file_pathv4 = r"D:\Progress Files\Non-Time Progress\Endgame\Experiment"

with DAG(
    dag_id = 'experiment_environment_test',
    description = 'Endgame experiment',
    default_args=args,
    schedule_interval='* * * * *') as dag:
    dummy_task = DummyOperator(
        task_id = 'initial_task'
    )

    experiment_task = BashOperator(
        task_id = 'experiment_writer',
        bash_command=f"set -e; python /opt/airflow/dags/file/experiment.py" #‘{{ next_execution_date }}’
        # Fucking Weird: Assuming dag is in dags directory, python script in jupyter.
        # Does running jupyter nbconvert really necessary?
        ## Placing the file inside .dags is enough, why need to exec -it again.
        ## How WSL docker-airflow and Jupyter terminal connected in the first place?
        ## Why need those if in the end it get placed inside the script inside dag folder.
        ## I only need those because i don't have python executor. therefore placing it in jupyter domain.
        ## The only thing Rahmat do to connect python and airflow is just by activating venv, how is that possible? how WSL connect to python?
        ## It seems the only way it all connect because Rahmat use single encompassing connector MobaXterm, but i am not very sure.
        ## NO SIGN OF USING JUPYTER NBCONVERT AGAIN AFTER THE SUGGESTION

        # setting experiment.py inside 'file' folder: python: can't open file '/mnt/d/Progress Files/Non-Time Progress/Endgame/Experiment/***/dags/file/experiment.py': [Errno 2] No such file or directory
        # f"set -e; python {file_pathv4}" SyntaxError: (unicode error) 'unicodeescape' codec can't decode bytes in position 17-18: malformed \N character escape
        # f"set -e; python /mnt/d/Progress\ Files/Non-Time\ Progress/Endgame/Experiment/experiment.py" #--> Error: python: can't open file '/mnt/d/Progress Files/Non-Time Progress/Endgame/Experiment/experiment.py': [Errno 2] No such file or directory
        # f"set -e; python /mnt/d/Progress\ Files/Non-Time\ Progress/Endgame/Experiment/airflow/dags/experiment.py" #--> Error: python: can't open file '/mnt/d/Progress Files/Non-Time Progress/Endgame/Experiment/***/dags/experiment.py': [Errno 2] No such file or directory
        # f"set -e; python experiment.py"" #--> Error, file or directory not found.
        # f"""jupyter nbconvert --execute --clear-output experiment.py""" #--> Error: bash : jupyter command not found.
        # f"""python {file_pathv3}""" #Both using 'r' and not, Error: INFO - python: can't open file '/mnt/d/Progress': [Errno 2] No such file or directory
        # f"""python --execute --clear-output experiment.py"""     #Error: unknown option --execute. Why you put execute for python dumbass?
        # """python experiment.py"""
        # f"jupyter nbconvert --to notebook --execute {file_pathv2}"
        # jupyter nbconvert --to notebook --execute executor_scripts/experiment.py"""
        # f"""jupyter nbconvert --to notebook --execute /mnt/c/Users/ALFANSURI/Digital Skola/Final_Project/executor_scripts/experiment.py"""
        #  'conda activate chapter37_SparkStreaming' | source /mnt/c/Users/ALFANSURI/anaconda3\ && 
        # bash_command = "c:/Users/ALFANSURI/AppData/Local/Programs/Python/Python39/python.exe \
        #     'd:/Progress Files/Non-TIme Progress/Endgame/Experiment/Covid19APIIngestor/experiment.py'"
    )

    dummy_task >> experiment_task
#if __name__ == __main__: