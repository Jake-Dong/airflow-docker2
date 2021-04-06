import xml.etree.ElementTree as ET
import epo_ops
import csv
import time
from datetime import datetime, timedelta
import pandas as pd
import pymysql
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

def citation_csv():
    yesterday = datetime.today() - timedelta(9)
    date = yesterday.strftime("%Y%m%d")
    # DB 에서 pubilcation 정보를 불러오는 코드.
    conn =pymysql.connect(host='172.19.240.1', user='root', password= 'admin', database='test')
    cur = conn.cursor()
    sql = "SELECT * FROM pub_num_data_{}".format(date)
    cur.execute(sql)
    conn.commit()
    conn.close()
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

    # load_csv = pd.read_csv('./date_pub_num20190617.csv').head(20)
    #
    # country_list = load_csv['pub_country']
    # app_doc_num_list = load_csv['pub_doc_number']
    # kind_list = load_csv['pub_kind_code']

    client = epo_ops.Client(key='o2TgZqLMPnGxmiFk7rUUB0bTq9VZDbe1', secret='ZHYxcGxi9UprBTUD')

    citation_all_list = []
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

            xmlStr = response.text
            roots = ET.fromstring(xmlStr)
            pub_number = country+str(doc_num)+kind

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

            my_e_set = list(set(citaion_e_list))
            count_citaion_e_list = str(len(my_e_set))
            my_e_set_join = '|'.join(my_e_set)


            my_a_set = list(set(citaion_a_list))
            count_citaion_a_list = str(len(my_a_set))
            my_a_set_join = '|'.join(my_a_set)

            citation_all_list.append([pub_number
                                      ,my_e_set_join
                                      ,count_citaion_e_list
                                      ,my_a_set_join
                                      ,count_citaion_a_list])
        Refilename = './mnt/C/User/ehd5538/CSV_test/citation_csv_test_{}.csv'.format(date)
        f = open(Refilename, 'w', encoding='utf-8', newline='')
        csvWriter = csv.writer(f)
        csvWriter.writerow(['pub_number'
                               ,'citaion_e_list'
                                ,'count_citaion_e_list'
                                , 'citaion_a_list'
                                , 'count_citaion_a_list'
                                   ])
        for w in citation_all_list:
            csvWriter.writerow(w)
        f.close()
        print('완료')

def citation_to_DB():
    yesterday = datetime.today() - timedelta(9)
    date = yesterday.strftime("%Y%m%d")

    data = pd.read_csv('./mnt/C/User/ehd5538/CSV_test/citation_csv_test_{}.csv'.format(date))
    df = pd.DataFrame(data)

    conn = pymysql.connect(
        host='172.19.240.1'
        , user='root'
        , password='admin'
        , database='test'
    )
    cur = conn.cursor()
    for row in df.itertuples():
        sql = "INSERT INTO citation_info VALUES(%s,%s,%s,%s,%s);"
        val = row.pub_number, row.citaion_e_list_join, row.count_citaion_e_list, row.citaion_a_list_join, row.count_citaion_a_list
        cur.execute(sql, val)
        conn.commit()

    conn.close()

default_dag_args = {
    "owner": "airflow",
    "start_date": datetime.today()
}
dag = DAG(
    dag_id='citation_csv_db'
    , default_args=default_dag_args
    , schedule_interval='0 9 * * *'
    # , schedule_interval=timedelta(1)
)
task1 = PythonOperator(
    task_id='citation_csv'
    , python_callable=citation_csv
    , dag=dag
)

task2 = PythonOperator(
    task_id='citation_to_DB'
    , python_callable=citation_to_DB
    , dag=dag
)

task1 >> task2





