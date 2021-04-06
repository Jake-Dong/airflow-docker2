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

def New_citaion_DB():
    start = time.time()
    yesterday = datetime.today() - timedelta(7)
    date = '20210324'
    host_ip = '192.168.112.1'
    citaion_all_list = []
    # DB 에서 pubilcation 정보를 불러오는 코드.
    try:
        conn = pymysql.connect(
            host=host_ip
            , user='root'
            , password='admin'
            , database='test'
        )
        cur = conn.cursor()
        sql = "SELECT * FROM pub_num_data_{} LIMIT 1000".format(date)
        cur.execute(sql)
        conn.commit()
    except Exception as ex:
        print(ex,"EPO API 요청 오류 입니다.")
    else:
        datas = cur.fetchall()

        # 불러온 publication 정보를 for 반목문을 돌리기위해 list 에 넣는 코드.
        app_doc_num_list = []
        country_list = []
        kind_list = []
        simple_family_id = []
        for data in datas:
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
        for doc_num, country, kind in zip(app_doc_num_list, country_list, kind_list):

            response = client.published_data(
                reference_type='publication'
                , input=epo_ops.models.Docdb(str(doc_num), country, kind)
                , endpoint='biblio'
            )

            biblio_list = []
            xmlStr = response.text
            roots = ET.fromstring(xmlStr)

            pub_doc_number = country + str(doc_num) + kind
            print(pub_doc_number)
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
            my_e_set = list(set(citaion_e_list))
            count_citaion_e_list = str(len(my_e_set))

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
            my_a_set = list(set(citaion_a_list))
            count_citaion_a_list = str(len(my_a_set))

            citaion_e_list_join = '|'.join(my_e_set)
            count_citaion_e_list_join = '|'.join(count_citaion_e_list)

            citaion_a_list_join = '|'.join(my_a_set)
            count_citaion_a_list_join = '|'.join(count_citaion_a_list)


            citaion_all_list.append([pub_doc_number
                                     ,citaion_e_list_join
                                     ,count_citaion_e_list_join
                                     ,citaion_a_list_join
                                     ,count_citaion_a_list_join])
        try:
            conn = pymysql.connect(
                host=host_ip
                , user='root'
                , password='admin'
                , database='test'
            )
            cur = conn.cursor()
            for row in citaion_all_list:
                sql_1 = "INSERT INTO citation_info_test_1 VALUES(%s,%s,%s,%s,%s);"
                val = row
                cur.execute(sql_1, val)
                conn.commit()

        except Exception as ex:
            print(ex,"sql 코드 문제입니다.")
        finally:
            conn.close()
        print(time.time()-start)
default_dag_args = {
    "owner": "airflow",
    "start_date": datetime.today()
}
dag = DAG(
    dag_id='New_citaion_DB'
    , default_args=default_dag_args
    , schedule_interval='0 9 * * *'
    # , schedule_interval=timedelta(1)
)
task1 = PythonOperator(
    task_id='New_citaion_DB'
    , python_callable=New_citaion_DB
    , dag=dag
)


