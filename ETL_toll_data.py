# import the libraries
import datetime as dt
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator


# DAG Arguments
default_args = {
    'owner': '<replace-with-any-name>',
    'start_date': dt.datetime.today(),
    'email': ['<replace-with-any-email>'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# DAG Definition
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)


# Task Definition

# define the first task named unzip_data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvzf /home/project/airflow/dags/etlproject/tolldata.tgz -C /home/project/airflow/dags/etlproject',
    dag=dag,
)


# define the task to extract data from csv file
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 /home/project/airflow/dags/etlproject/vehicle-data.csv > /home/project/airflow/dags/etlproject/csv_data.csv',
    dag=dag,

)


# define the task to extract data from tsv file
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='tr "\\t" "," < /home/project/airflow/dags/etlproject/tollplaza-data.tsv | cut -d$"," -f5-7 > /home/project/airflow/dags/etlproject/tsv_data.csv',
    dag=dag,
)


# define the task to extract data from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 59-62,63-67 /home/project/airflow/dags/etlproject/payment-data.txt > /home/project/airflow/dags/etlproject/fixed_width_data.csv',
    dag=dag,
)


# define a task to consolidate data extracted from previous tasks
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste /home/project/airflow/dags/etlproject/csv_data.csv /home/project/airflow/dags/etlproject/tsv_data.csv /home/project/airflow/dags/etlproject/fixed_width_data.csv > /home/project/airflow/dags/etlproject/extracted_data.csv',
    dag=dag,
)


# define a task to Transform and load the data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="awk 'BEGIN {FS=OFS=\",\"} { $4= toupper($4) } 1' /home/project/airflow/dags/etlproject/extracted_data.csv > /home/project/airflow/dags/etlproject/staging/transformed_data.csv",
    dag=dag,
)


# Define task pipelines
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
