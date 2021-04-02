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


def citation_insert_DB():
    ip_ad = '172.17.112.1'
    user_id = 'airflow'
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


    client = epo_ops.Client(key='o2TgZqLMPnGxmiFk7rUUB0bTq9VZDbe1', secret='ZHYxcGxi9UprBTUD')
    biblio_all_list = []
    for doc_num,country,kind in zip(app_doc_num_list,country_list,kind_list):
        try:
            response = client.published_data(
                reference_type='publication'
                ,input=epo_ops.models.Docdb(str(doc_num),country,kind)
                ,endpoint='biblio'
            )
        except Exception as ex:
            print(ex)
        else:

            biblio_list = []
            xmlStr = response.text
            roots = ET.fromstring(xmlStr)
            pub_number = country+str(doc_num)+kind
            print(pub_number)
            # ------------------------------------citation  추출 영역 심사관일때------------------------------------
            citaion_e_list = []
            for citaion in roots.iter('{http://www.epo.org/exchange}citation'):
                test = citaion.attrib.get('cited-by')

                if test.startswith('e') == True:
                    for patcit in citaion.iter('{http://www.epo.org/exchange}patcit'):
                        country = patcit[1][0].text
                        doc_number = patcit[1][1].text
                        kind = patcit[1][2].text
                        citaion_e_list.extend([country + doc_number + kind])
                else:
                    pass
            my_e_set = set(citaion_e_list)
            citaion_e_list = list(my_e_set)
            count_citaion_e_list = str(len(citaion_e_list))

            # ------------------------------------citation  추출 영역 본인 일때------------------------------------

            citaion_a_list = []
            for citaion in roots.iter('{http://www.epo.org/exchange}citation'):
                test = citaion.attrib.get('cited-by')

                if test.startswith('a') == True:
                    for patcit in citaion.iter('{http://www.epo.org/exchange}patcit'):
                        country = patcit[1][0].text
                        doc_number = patcit[1][1].text
                        kind = patcit[1][2].text
                        if kind == None:
                            kind = ' '
                        citaion_a_list.extend([country + doc_number + kind])
                else:
                    pass
            my_a_set = set(citaion_a_list)
            citaion_a_list = list(my_a_set)
            count_citaion_a_list = str(len(citaion_a_list))

            citaion_e_list_join = '|'.join(citaion_e_list)
            count_citaion_e_list_join ='|'.join(count_citaion_e_list)

            citaion_a_list_join = '|'.join(citaion_a_list)
            count_citaion_a_list_join = '|'.join(count_citaion_a_list)


#             ==================
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
                    sql_1 = "INSERT INTO citation_info VALUES(%s,%s,%s,%s,%s);"
                    val = pub_number, citaion_e_list_join, count_citaion_e_list, citaion_a_list_join, count_citaion_a_list
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
    dag_id='citation'
    , default_args=default_dag_args
    , schedule_interval='0 9 * * *'
    # , schedule_interval=timedelta(1)
)
task1 = PythonOperator(
    task_id='citation'
    , python_callable=citation_insert_DB
    , dag=dag
)
