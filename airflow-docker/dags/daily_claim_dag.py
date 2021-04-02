import xml.etree.ElementTree as ET
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


def claim_insert_DB():
    ip_ad = '172.17.112.1'
    user_id = 'airflow'
    client = epo_ops.Client(key='o2TgZqLMPnGxmiFk7rUUB0bTq9VZDbe1', secret='ZHYxcGxi9UprBTUD')

    yesterday = datetime.today() - timedelta(9)

    date = yesterday.strftime("%Y%m%d")
    try:
        conn = pymysql.connect(
            host=ip_ad
            , user=user_id
            , port=3306
            , password='admin'
            , database='test'

        )
        # Connection 으로부터 Cursor 생성
        cur = conn.cursor()

        try:
            sql = "SELECT * FROM pub_num_data_{}".format(date)
            cur.execute(sql)

            # 데이타 Fetch
            pub_infos = cur.fetchall()
            # print(rows)  # 전체 rows
            conn.commit()

        except Exception:
            print("Error in MySQL query")
        finally:
            conn.close()
    except Exception:
        print("Error in MySQL connexion")


    # 불러온 publication 정보를 for 반목문을 돌리기위해 list 에 넣는 코드.
    app_doc_num_list = []
    country_list = []
    kind_list = []
    simple_family_id = []
    for data in pub_infos:

        family_id = data[1]
        pub_country = data[2]
        pub_number = data[3]
        pub_kind = data[4]

        app_doc_num_list.append(pub_number)
        country_list.append(pub_country)
        kind_list.append(pub_kind)
        simple_family_id.append(family_id)

    claims_all_list = []
    for doc_num,country,kind in zip(app_doc_num_list,country_list,kind_list):
        pub_num = country+str(doc_num)+kind
        print(pub_num)
        claims_all_list = []
        try:
            response = client.published_data(
                    reference_type = 'publication'
                    , input = epo_ops.models.Docdb(str(doc_num),country,kind)
                    ,endpoint='claims'
            )
        except Exception as ex:
            print(ex)
        else:
            xmlStr = response.text
            roots = ET.fromstring(xmlStr)

            for claims in roots.iter('{http://www.epo.org/fulltext}claim-text'):
                claims_text = claims.text.replace('/','-')
                claims_all_list.append(claims_text)


            claims_all_join = '|'.join(claims_all_list)

            try:
                conn = pymysql.connect(
                    host=ip_ad
                    , user=user_id
                    , port=3306
                    , password='admin'
                    , database='test'
                )
                # Connection 으로부터 Cursor 생성
                cur = conn.cursor()
                try:
                    sql_1 = "INSERT INTO claims_info VALUES(%s,%s);"
                    val = pub_num, claims_all_join
                    cur.execute(sql_1, val)
                    # 데이타 Fetch
                    conn.commit()
                except Exception:
                    print("Error in MySQL query")
                finally:
                    conn.close()
            except Exception:
                print("Error in MySQL connexion")
default_dag_args = {
    "owner": "airflow",
    "start_date": datetime.today()
}
dag = DAG(
    dag_id='claims'
    , default_args=default_dag_args
    , schedule_interval='0 9 * * *'
    # , schedule_interval=timedelta(1)
)
task1 = PythonOperator(
    task_id='claim'
    , python_callable=claim_insert_DB
    , dag=dag
)
