from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago
import boto3

def trigger_glue_job(job_name):
    client = boto3.client('glue')
    client.start_job_run(
        JobName=job_name,
    )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id = 'sentiment_analysis_dag',
    default_args = default_args,
    default_view = 'graph',
    schedule_interval='@hourly'
) as dag:

    start = DummyOperator(
        task_id = 'begin_exec',
        dag = dag
    )

    clense_data = PythonOperator(
        task_id='clense_data',
        python_callable=trigger_glue_job,
        op_kwargs={
            'job_name': 'clense_data'
        },
        dag=dag
    )

    ml_model = BashOperator(
        task_id = 'run_ml_model',
        bash_command = '''
            jupyter nbconvert --execute /home/ec2-user/SageMaker/sentiment_analysis.ipynb
            echo "Ran the model!"
        ''',
        dag=dag
    )

    end = DummyOperator(
        task_id = 'end_exec',
        dag = dag
    )

    start >> clense_data >> ml_model >> end