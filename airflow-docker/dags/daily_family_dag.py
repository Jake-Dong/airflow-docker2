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


def family_insert_DB():
    # DB 에서 수집이 완료된 pubilcation 정보를 불러오는 코드.
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
    family_all_list = []

    for simple_family_id_one,doc_num,country_list,kind_list in zip(simple_family_id,app_doc_num_list,country_list,kind_list):
        try:
            response = client.family(
                reference_type='publication'
                ,input=epo_ops.models.Docdb(str(doc_num),country_list,kind_list)
            )
        except Exception as ex:
            print(ex)
        else:

            xmlStr = response.text
            roots = ET.fromstring(xmlStr)

            # ----------------------------simple doc family-id--------------------------------
            sim_fam_id = simple_family_id

            # ----------------------------원출원 번호 출력 --------------------------------

            doc_number = country_list+str(doc_num)+kind_list
            print(doc_number)
            family_id_list = []
            simple_family_pub_number_list = []
            simple_family_pub_country_list = []
            inp_family_pub_number_list = []
            inp_family_pub_country_list = []
            # ----------------------------simple family 출력------------------------------
            for family_member in roots.iter('{http://ops.epo.org}family-member'):
                family_member_id = family_member.attrib.get('family-id')
                family_member_id_set = list(set(family_member_id))
                family_id_list.extend([family_member_id_set])
                if str(simple_family_id_one) == str(family_member_id):
                    pub_country=family_member[0][0][0].text
                    pub_doc_number=family_member[0][0][1].text
                    pub_kind=family_member[0][0][2].text
                    simple_family_pub_number_list.extend([
                                    pub_country
                                    + pub_doc_number
                                    + pub_kind
                                ])
                    simple_family_pub_country_list.extend([pub_country])
                # ----------------------------inpadoc family 출력------------------------------
                else:
                    pub_country=family_member[0][0][0].text
                    pub_doc_number=family_member[0][0][1].text
                    pub_kind=family_member[0][0][2].text
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
                count_simple_family_pub_number_list ='null'

            if count_simple_family_pub_country_list == 0:
                count_simple_family_pub_country_list= 'null'

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
                    sql_1 = "INSERT INTO family_info VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                    val = simple_family_pub_number_list_join, count_simple_family_pub_number_list, simple_family_pub_country_set_list_join, count_simple_family_pub_country_list, inp_family_pub_number_list_join, count_inp_family_pub_number_list, inp_family_pub_country_set_list_join, count_inp_family_pub_country_list, doc_number
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
    dag_id='family'
    , default_args=default_dag_args
    , schedule_interval='0 9 * * *'
    # , schedule_interval=timedelta(1)
)
task1 = PythonOperator(
    task_id='family'
    , python_callable=family_insert_DB
    , dag=dag
)
