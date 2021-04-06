import xml.etree.ElementTree as ET
import epo_ops
import csv
import time
from datetime import datetime, timedelta
import pandas
import pymysql
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import os


def csv_test():
    print(__file__)
    print(os.path.realpath(__file__))
    print(os.path.abspath(__file__))
    any_list = ['1','2','3','4','5']

    Refilename = '/usr/local/airflow/dags/csv_file/csv_test.csv'
    f = open(Refilename, 'w', encoding='utf-8', newline='')
    csvWriter = csv.writer(f)
    csvWriter.writerow([
        'doc_number'
    ])
    for w in any_list:
        csvWriter.writerow(w)
    f.close()
    print('완료')



default_dag_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 3, 28)
}
dag = DAG(
    # webserver 에서 보여지는 id
    dag_id='csv'
    # 설정값 입력
    , default_args=default_dag_args
    # 크론탭으로 스캐줄 관리.
    , schedule_interval='0 1 * * *'
    # , schedule_interval=timedelta(1)
)
task1 = PythonOperator(
    task_id='csv'
    , python_callable= csv_test
    , dag=dag
)


