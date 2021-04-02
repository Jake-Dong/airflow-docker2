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


def lv2_fun(Lv_1_cpc):
    cpc_lv1_all = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'Y']

    cpc_lv2_A = ['A01', 'A21', 'A22', 'A23', 'A24', 'A41', 'A42', 'A43', 'A44', 'A45', 'A46', 'A47', 'A61', 'A62',
                 'A63']
    cpc_lv2_B = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B21', 'B22', 'B23', 'B24', 'B25',
                 'B26', 'B27', 'B28', 'B29', 'B30', 'B31', 'B32', 'B33', 'B41', 'B42', 'B43', 'B44', 'B60', 'B61',
                 'B62', 'B63', 'B64', 'B65', 'B66', 'B67', 'B68', 'B81', 'B82']
    cpc_lv2_C = ['C01', 'C02', 'C03', 'C04', 'C05', 'C06', 'C07', 'C08', 'C09', 'C10', 'C11', 'C12', 'C13', 'C14',
                 'C21', 'C22', 'C23', 'C25', 'C30', 'C40']
    cpc_lv2_D = ['D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'D10', 'D21']
    cpc_lv2_E = ['E01', 'E02', 'E03', 'E04', 'E05', 'E06', 'E21']
    cpc_lv2_F = ['F01', 'F02', 'F03', 'F04', 'F05', 'F15', 'F16', 'F17', 'F21', 'F22', 'F23', 'F24', 'F25', 'F26',
                 'F27', 'F28', 'F41', 'F42']
    cpc_lv2_G = ['G01', 'G02', 'G03', 'G04', 'G05', 'G06', 'G07', 'G08', 'G09', 'G10', 'G11', 'G12', 'G16', 'G21']
    cpc_lv2_H = ['H01', 'H02', 'H03', 'H04', 'H05']
    cpc_lv2_Y = ['Y02', 'Y04', 'Y10']

    cpc_lv3_all = ['H', 'N', 'L', 'J', 'Q', 'Y', 'B', 'D', 'P', 'M', 'V', 'C', 'F', 'K', 'G']

    if Lv_1_cpc == 'A':
        return cpc_lv2_A
    elif Lv_1_cpc == 'B':
        return cpc_lv2_B
    elif Lv_1_cpc == 'C':
        return cpc_lv2_C
    elif Lv_1_cpc == 'D':
        return cpc_lv2_D
    elif Lv_1_cpc == 'E':
        return cpc_lv2_E
    elif Lv_1_cpc == 'F':
        return cpc_lv2_F
    elif Lv_1_cpc == 'G':
        return cpc_lv2_G
    elif Lv_1_cpc == 'H':
        return cpc_lv2_H
    elif Lv_1_cpc == 'Y':
        return cpc_lv2_Y


def pub_num_DB():
    ip_ad = '172.17.112.1'
    user_id = 'airflow'
    #  EPO 회원 key 를 받아 넣어준다 EPO 서버 트래픽 관리차원.
    client = epo_ops.Client(
        key='o2TgZqLMPnGxmiFk7rUUB0bTq9VZDbe1',
        secret='ZHYxcGxi9UprBTUD'
    )

    #  https://register.epo.org/help?lng=en&topic=countrycodes  epo 나라 분류 코드  반목문 작업시 시 참고
    #  cpc 정보가 있는 파일을 중복제거 , 슬라이스 하여 Lv 별로 나누어서 리스트로 놓았다.
    #  이유는 해당 코드로 총 결과(total result)가 2000개 이하로 나오게하기위한 cql 조건에 필요하기 때문이다.



    country_code =['AL','AP','AR','AT','AU','BA','BE','BG','BR','CA','CH','CL','CN','CO','CR','CS','CU','CY','CZ','DD','DE','DK','DZ','EA','EC','EE','EG','EP','ES','FI','FR','GB','GC','GE','GR','GT','HK','HR','HU','ID','IE','IL','IN','IS','IT','JP','KE','KR','LI','LT','LU','LV','MA','MC','MD','MK','MN','MT','MW','MX','MY','NC','NI','NL','NO','NZ','OA','PA','PE','PH','PL','PT','RO','RU','SE','SG','SI','SK','SU','SV ','TJ','TR','TT','TW','UA','US','UY','VN','WO','YU','ZA','ZM','ZW']
    # 수집을 원하는 날짜를 반복입력 받는 리스트 함수.
    # dt_index = pandas.date_range(start='20190609', end='20190610')
    # daet_list = list(dt_index.strftime("%Y%m%d"))

    yesterday = datetime.today() - timedelta(7)

    date = yesterday.strftime("%Y%m%d")

    #  해당 리스트는 total_result 가 2000 이하 예를들어 300 개 일때 20번의 반복문을 돌면 비효율적이기때문에 아래의 수식으로 최적의 반복문만 돌기위한 사전작업이다.
    begin_max_num_list = ['1', '100', '200', '300', '400', '500', '600', '700', '800', '900', '1000', '1100', '1200', '1300',
                     '1400', '1500', '1600', '1700', '1800', '1900']
    end_max_num_list = ['99', '199', '299', '399', '499', '599', '699', '799', '899', '999', '1099', '1199', '1299', '1399',
                   '1499', '1599', '1699', '1799', '1899', '1999']

    pub_all_list = []
    begin_num_list = []
    end_num_list = []

    cpc_lv1_list =['A','B','C','D','E','F','G','H','Y']
    cpc_lv3_all = ['A','B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'W', 'Y']

    #  원하는 date list 를 time 모듈로 받고

    conn = pymysql.connect(
        host=ip_ad
        , user=user_id
        , port=3306
        , password='admin'
        , database='test'
    )
    cur = conn.cursor()
    sql = """CREATE TABLE test.pub_num_data_{} (date varchar(200),family_id varchar(200),pub_country varchar(200),pub_doc_number varchar(200),pub_kind_code varchar(200));""".format(date)
    cur.execute(sql)
    conn.commit()
    conn.close()
    print(cpc_lv1_list)
    for cql1 in cpc_lv1_list:
        print(cql1)
        print('lv1 시작')
        try:
            response = client.published_data_search(
                cql='pd={date} and (ipc={cql} or cpc={cql})'.format(date=date,cql=cql1)
                , range_begin= 1
                , range_end= 1
                , constituents=['biblio']
            )
        except Exception as ex:
            print(ex)
            pass
        else:
            xmlStr = response.text
            roots = ET.fromstring(xmlStr)

            lv1_begin_num_list = []
            lv1_end_num_list = []

            for total_result1 in roots.iter('{http://ops.epo.org}biblio-search'):
                total_result_count1 = int(total_result1.attrib.get('total-result-count'))
                print('lv1 total', total_result_count1, ' date=', date, ' cql=', cql1)

                if total_result_count1 < 2000:
                    num_list = int(total_result_count1 / 100)
                    begin_num = begin_max_num_list[:num_list + 1]
                    end_num = end_max_num_list[:num_list + 1]

                    lv1_begin_num_list.extend(begin_num)
                    lv1_end_num_list.extend(end_num)
                    for begin_num, end_num in zip(lv1_begin_num_list, lv1_end_num_list):
                        response = client.published_data_search(
                            cql='pd={date} and (ipc={cql} or cpc={cql})'.format(date=date, cql=cql1)
                            , range_begin=begin_num
                            , range_end=end_num
                            , constituents=['biblio']
                        )
                        xmlStr = response.text
                        roots = ET.fromstring(xmlStr)

                        for exchange_document in roots.iter('{http://www.epo.org/exchange}exchange-document'):
                            doc_simple_id = exchange_document.attrib.get('family-id')
                            pub_country = exchange_document.attrib.get('country')
                            pub_doc_number = exchange_document.attrib.get('doc-number')
                            pub_kind = exchange_document.attrib.get('kind')
                            pub_list = [doc_simple_id, pub_country, pub_doc_number, pub_kind]
                            pub_all_list.append(pub_list)

                            conn = pymysql.connect(
                                host=ip_ad
                                , user=user_id
                                , port=3306
                                , password='admin'
                                , database='test'

                            )
                            cur = conn.cursor()
                            sql = """INSERT INTO pub_num_data_{} VALUES(%s,%s,%s,%s,%s);""".format(date)
                            val = date,doc_simple_id, pub_country, pub_doc_number, pub_kind
                            cur.execute(sql, val)
                            conn.commit()
                            connect_timeout = 1
                            conn.close()

                else:
                    lv2_list = list(lv2_fun(cql1))
                    print(lv2_list)
                    for cql2 in lv2_list:
                        print('lv2 시작')
                        try:
                            response = client.published_data_search(
                                cql='pd={date} and (ipc={cql} or cpc={cql})'.format(date=date, cql=cql2)
                                , range_begin=1
                                , range_end=1
                                , constituents=['biblio']
                            )
                        except:
                            pass
                        else:

                            xmlStr = response.text
                            roots = ET.fromstring(xmlStr)

                            lv2_begin_num_list = []
                            lv2_end_num_list = []

                            for total_result2 in roots.iter('{http://ops.epo.org}biblio-search'):
                                total_result_count2 = int(total_result2.attrib.get('total-result-count'))
                                print('lv2 total', total_result_count2, ' date=', date, ' cql=', cql2)
                                if total_result_count2 < 2000:
                                    num_list = int(total_result_count2 / 100)
                                    begin_num = begin_max_num_list[:num_list + 1]
                                    end_num = end_max_num_list[:num_list + 1]

                                    lv2_begin_num_list.extend(begin_num)
                                    lv2_end_num_list.extend(end_num)
                                    for begin_num, end_num in zip(lv2_begin_num_list, lv2_end_num_list):
                                        response = client.published_data_search(
                                            cql='pd={date} and (ipc={cql} or cpc={cql})'.format(date=date, cql=cql2)
                                            , range_begin=begin_num
                                            , range_end=end_num
                                            , constituents=['biblio']
                                        )
                                        xmlStr = response.text
                                        roots = ET.fromstring(xmlStr)

                                        for exchange_document in roots.iter('{http://www.epo.org/exchange}exchange-document'):
                                            doc_simple_id = exchange_document.attrib.get('family-id')
                                            pub_country = exchange_document.attrib.get('country')
                                            pub_doc_number = exchange_document.attrib.get('doc-number')
                                            pub_kind = exchange_document.attrib.get('kind')

                                            conn = pymysql.connect(
                                                host=ip_ad
                                                , user=user_id
                                                , port=3306
                                                , password='admin'
                                                , database='test'

                                            )
                                            cur = conn.cursor()
                                            sql = """INSERT INTO pub_num_data_{} VALUES(%s,%s,%s,%s,%s);""".format(date)
                                            val = date, doc_simple_id, pub_country, pub_doc_number, pub_kind
                                            cur.execute(sql, val)
                                            conn.commit()
                                            conn.close()


                                else:
                                    re_cql3_list =[]
                                    for cql in cpc_lv3_all:
                                        re_cql = cql2+cql
                                        re_cql3_list.append(re_cql)
                                    print(re_cql3_list)
                                    for cql3 in re_cql3_list:

                                        print('lv3 시작')
                                        try:
                                            response = client.published_data_search(
                                                cql='pd={date} and (ipc={cql} or cpc={cql})'.format(date=date, cql=cql3)
                                                , range_begin=1
                                                , range_end=1
                                                , constituents=['biblio']
                                            )
                                        except:
                                            pass
                                        else:

                                            xmlStr = response.text
                                            roots = ET.fromstring(xmlStr)

                                            lv3_begin_num_list = []
                                            lv3_end_num_list = []

                                            for total_result3 in roots.iter('{http://ops.epo.org}biblio-search'):
                                                total_result_count3 = int(total_result3.attrib.get('total-result-count'))
                                                print('lv3 total', total_result_count3, ' date=', date, ' cql=', cql3)
                                                if total_result_count3 < 2000:
                                                    num_list = int(total_result_count3 / 100)
                                                    begin_num = begin_max_num_list[:num_list + 1]
                                                    end_num = end_max_num_list[:num_list + 1]

                                                    lv3_begin_num_list.extend(begin_num)
                                                    lv3_end_num_list.extend(end_num)
                                                    for begin_num, end_num in zip(lv3_begin_num_list, lv3_end_num_list):
                                                        response = client.published_data_search(
                                                            cql='pd={date} and (ipc={cql} or cpc={cql})'.format(date=date, cql=cql3)
                                                            , range_begin=begin_num
                                                            , range_end=end_num
                                                            , constituents=['biblio']
                                                        )
                                                        xmlStr = response.text
                                                        roots = ET.fromstring(xmlStr)
                                                        for exchange_document in roots.iter(
                                                                '{http://www.epo.org/exchange}exchange-document'):
                                                            doc_simple_id = exchange_document.attrib.get(
                                                                'family-id')
                                                            pub_country = exchange_document.attrib.get('country')
                                                            pub_doc_number = exchange_document.attrib.get(
                                                                'doc-number')
                                                            pub_kind = exchange_document.attrib.get('kind')
                                                            pub_list = [doc_simple_id, pub_country, pub_doc_number,
                                                                        pub_kind]
                                                            pub_all_list.append(pub_list)

                                                            conn = pymysql.connect(
                                                                host=ip_ad
                                                                , user=user_id
                                                                , port=3306
                                                                , password='admin'
                                                                , database='test'


                                                            )
                                                            cur = conn.cursor()
                                                            sql = """INSERT INTO pub_num_data_{} VALUES(%s,%s,%s,%s,%s);""".format(date)
                                                            val = date, doc_simple_id, pub_country, pub_doc_number, pub_kind
                                                            cur.execute(sql, val)
                                                            conn.commit()
                                                            conn.close()

                                                else:


                                                    for cql3 in re_cql3_list:
                                                        for coun in country_code:
                                                            try:
                                                                response = client.published_data_search(
                                                                    cql='pd={date} and (ipc={cql} or cpc={cql}) and ap={coun}'.format(
                                                                        date=date, cql=cql3, coun=coun)
                                                                    , range_begin=1
                                                                    , range_end=1
                                                                    , constituents=['biblio']
                                                                )
                                                            except:
                                                                pass
                                                            else:

                                                                xmlStr = response.text
                                                                roots = ET.fromstring(xmlStr)

                                                                coun_begin_num_list = []
                                                                coun_end_num_list = []

                                                                for total_result in roots.iter('{http://ops.epo.org}biblio-search'):
                                                                    total_result_count = int(
                                                                        total_result.attrib.get('total-result-count'))
                                                                    print('lv4 total', total_result_count, ' date=', date, ' cql=',
                                                                          cql3, ' coun=', coun)
                                                                    num_list = int(total_result_count / 100)

                                                                    begin_num = begin_max_num_list[:num_list + 1]
                                                                    end_num = end_max_num_list[:num_list + 1]

                                                                    coun_begin_num_list.extend(begin_num)
                                                                    coun_end_num_list.extend(end_num)
                                                                    for begin_num, end_num in zip(coun_begin_num_list, coun_end_num_list):

                                                                        response = client.published_data_search(
                                                                            cql='pd={date} and (ipc={cql} or cpc={cql} and ap={coun})'.format(
                                                                                date=date, cql=cql3, coun=coun)
                                                                            , range_begin=begin_num
                                                                            , range_end=end_num
                                                                            , constituents=['biblio']
                                                                        )

                                                                        xmlStr = response.text
                                                                        roots = ET.fromstring(xmlStr)

                                                                        for exchange_document in roots.iter(
                                                                                '{http://www.epo.org/exchange}exchange-document'):
                                                                            doc_simple_id = exchange_document.attrib.get('family-id')
                                                                            pub_country = exchange_document.attrib.get('country')
                                                                            pub_doc_number = exchange_document.attrib.get('doc-number')
                                                                            pub_kind = exchange_document.attrib.get('kind')
                                                                            conn = pymysql.connect(
                                                                                host=ip_ad
                                                                                , user=user_id
                                                                                , port=3306
                                                                                , password='admin'
                                                                                , database='test'
                                                                            )
                                                                            cur = conn.cursor()
                                                                            sql = """INSERT INTO pub_num_data_{} VALUES(%s,%s,%s,%s,%s);""".format(date)
                                                                            val = date, doc_simple_id, pub_country, pub_doc_number, pub_kind
                                                                            cur.execute(sql, val)
                                                                            conn.commit()
                                                                            conn.close()

default_dag_args = {
    "owner": "airflow",
    "start_date": datetime.today()
}
dag = DAG(
    dag_id='pub_num_DB'
    , default_args=default_dag_args
    , schedule_interval='0 1 * * *'
    # , schedule_interval=timedelta(1)
)
task1 = PythonOperator(
    task_id='pub_num'
    , python_callable= pub_num_DB
    , dag=dag
)
