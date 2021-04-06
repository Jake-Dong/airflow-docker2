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
def New_familt_DB():
    start = time.time()
    host_ip = '192.168.112.1'
    yesterday = datetime.today() - timedelta(7)

    date = '20210324'
    # db 에 저장되어있는 publication 정보를 가져오는 코드
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
        conn.close()
    except Exception as ex:
        print(ex,"DB 데이터 요청 오류 입니다.")
    datas = cur.fetchall()

    app_doc_num_list = []
    country_list = []
    kind_list = []
    family_id_list = []
    for data in datas:
        family_id = data[1]
        pub_country = data[2]
        pub_number = data[3]
        pub_kind = data[4]

        app_doc_num_list.append(pub_number)
        country_list.append(pub_country)
        kind_list.append(pub_kind)
        family_id_list.append(family_id)


    client = epo_ops.Client(key='o2TgZqLMPnGxmiFk7rUUB0bTq9VZDbe1', secret='ZHYxcGxi9UprBTUD')
    family_all_list = []

    for simple_family_id_one, doc_num, country_list, kind_list in zip(family_id_list, app_doc_num_list, country_list,
                                                                      kind_list):
        try:
            response = client.family(
                reference_type='publication'
                , input=epo_ops.models.Docdb(str(doc_num), country_list, kind_list)
            )
        except Exception as ex:
            print(ex,"EPO API 요청 오류 입니다.")
        else:
            xmlStr = response.text
            roots = ET.fromstring(xmlStr)

            # ----------------------------simple doc family-id--------------------------------
            # 처음 pub_info 를 가지고 올때 해당 family_id 도 같이 추출하여 simple_family_id 를 변수에 담는다 후에 inpadoc_family_id 를 골라 내기 위함이다.
            sim_fam_id = simple_family_id_one

            # ----------------------------원출원 번호 출력 --------------------------------

            doc_number = country_list + str(doc_num) + kind_list
            print(doc_number)
            family_id_list = []
            simple_family_pub_number_list = []
            simple_family_pub_country_list = []
            inp_family_pub_number_list = []
            inp_family_pub_country_list = []
            # ----------------------------simple family 출력------------------------------
            #  root.iter 를 이용해서 root 이하의 자식 태그를 모두 불러온다. 그리고 원하는 태그들만 추출하여 text 로 변환해 변수에 저장한다.
            for family_member in roots.iter('{http://ops.epo.org}family-member'):
                family_member_id = family_member.attrib.get('family-id')
                family_member_id_set = list(set(family_member_id))
                family_id_list.extend([family_member_id_set])
                # simple_fam_id 과 inpadoc_fam_id 를 나누기위한 if 조건문.
                # 각각의 패밀리id 를 기존 simple_fam_id 와 비교하여 맞는것은 simple  아닌것은 inpadoc 으로 판단하고 각각 따로 변수에 담는다.
                if str(simple_family_id_one) == str(family_member_id):
                    pub_country = family_member[0][0][0].text
                    pub_doc_number = family_member[0][0][1].text
                    pub_kind = family_member[0][0][2].text
                    simple_family_pub_number_list.extend([
                        pub_country
                        + pub_doc_number
                        + pub_kind
                    ])
                    simple_family_pub_country_list.extend([pub_country])
                # ----------------------------inpadoc family 출력------------------------------
                else:
                    pub_country = family_member[0][0][0].text
                    pub_doc_number = family_member[0][0][1].text
                    pub_kind = family_member[0][0][2].text
                    inp_family_pub_number_list.extend([
                        pub_country
                        + pub_doc_number
                        + pub_kind
                    ])
                    inp_family_pub_country_list.extend([pub_country])

            count_simple_family_pub_number_list = len(simple_family_pub_number_list)
            count_simple_family_pub_country_list = len(set(simple_family_pub_country_list))
            simple_family_pub_country_set_list = list(set(simple_family_pub_country_list))
            count_inp_family_pub_number_list = len(inp_family_pub_number_list)
            count_inp_family_pub_country_list = len(set(inp_family_pub_country_list))
            inp_family_pub_country_set_list = list(set(inp_family_pub_country_list))

            if count_simple_family_pub_number_list == 0:
                count_simple_family_pub_number_list = 'null'

            if count_simple_family_pub_country_list == 0:
                count_simple_family_pub_country_list = 'null'

            if count_inp_family_pub_number_list == 0:
                count_inp_family_pub_number_list = 'null'

            if count_inp_family_pub_country_list == 0:
                count_inp_family_pub_country_list = 'null'
            else:
                pass

            # DB 에 적재 하려면 콤마구분자를 바꾸어줘야 함. 이유는 DB에서의 콤마명령어가 존재하기때문에 언어 명령이 꼬일수 있기때문.

            simple_family_pub_number_list_join = '|'.join(simple_family_pub_number_list)
            simple_family_pub_country_set_list_join = '|'.join(simple_family_pub_country_set_list)

            inp_family_pub_number_list_join = '|'.join(inp_family_pub_number_list)
            inp_family_pub_country_set_list_join = '|'.join(inp_family_pub_country_set_list)

            family_all_list.append([doc_number
                                       ,simple_family_pub_number_list_join
                                       ,count_simple_family_pub_number_list
                                       ,simple_family_pub_country_set_list_join
                                       ,count_simple_family_pub_country_list
                                       ,inp_family_pub_number_list_join
                                       ,count_inp_family_pub_number_list
                                       ,inp_family_pub_country_set_list_join
                                       ,count_inp_family_pub_country_list])
        try:
            conn = pymysql.connect(
                host=host_ip
                , user='root'
                , password='admin'
                , database='test'
            )
            cur = conn.cursor()
            for row in family_all_list:
                sql_1 = "INSERT INTO family_info_test_1 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);"
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
    dag_id='New_familt_DB'
    , default_args=default_dag_args
    , schedule_interval='0 9 * * *'
    # , schedule_interval=timedelta(1)
)
task1 = PythonOperator(
    task_id='New_familt_DB'
    , python_callable=New_familt_DB
    , dag=dag
)
