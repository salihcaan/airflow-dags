# datetime
from datetime import timedelta, datetime

# The DAG object
from airflow import DAG

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# initializing the default arguments
default_args = {
    'owner': 'Ranga',
    'start_date': datetime(2022, 3, 4),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
hello_world_dag = DAG('example_dag_2',
                      default_args=default_args,
                      description='This is a simple description',
                      schedule_interval='* * * * *',
                      catchup=False,
                      tags=['example, data-platform']
                      )


# python callable function
def print_hello():
    return 'Hello World!'


# Creating first task
start_task = DummyOperator(task_id='start_task', dag=hello_world_dag)

# Creating second task
hello_world_task = PythonOperator(task_id='example_task_1', python_callable=print_hello, dag=hello_world_dag)
another_task = PythonOperator(task_id='another_task', python_callable=print_hello, dag=hello_world_dag)

# Creating third task
end_task = DummyOperator(task_id='end_task', dag=hello_world_dag)

# Set the order of execution of tasks.
# start_task >> hello_world_task >> end_task
