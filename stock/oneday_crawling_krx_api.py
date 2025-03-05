import subprocess
import datetime
import argparse
import sys
from configparser import ConfigParser
import time
import logging
import pandas as pd

import sys
import os


current_path = os.path.abspath(__file__)
sys.path.append("/".join(current_path.split("/")[:-1])+'/../util')
from MongoDBSingleton import MongoDBSingleton
from PostgresSingleton import PostgresSingleton

logger = logging.getLogger(name='krx api data crawling')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('|%(asctime)s||%(name)s||%(levelname)s|%(message)s',datefmt='%Y-%m-%d %H:%M:%S') 
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

import requests

import os
vault_addr = os.getenv('VAULT_ADDR')
vault_token = os.getenv('VAULT_TOKEN')


def get_vault_configuration(endpoint):
    endpoint = f"{vault_addr}/v1/kv/data/{endpoint}"

    # HTTP GET 요청을 통해 데이터를 가져옵니다.
    headers = {"X-Vault-Token": vault_token}
    response = requests.get(endpoint, headers=headers)

    if response.status_code == 200:
        data = response.json()
        return data['data']['data']

    else:
        # 에러 응답의 경우 예외를 발생시킵니다.
        response.raise_for_status()

def get_mongo_collection(postgres):
    mongo_collection = {}
    for collection_name,collection_kr_name in postgres.execute_query("SELECT collection_name,collection_kr_name FROM mongo_collection ", fetch_all=True):
        mongo_collection[collection_name] = collection_kr_name
    return mongo_collection

def get_api_description(postgres):
    api_detail = {}
    for api_name,kr_name,en_name,api_index in postgres.execute_query("SELECT api_name,kr_name,en_name,api_index FROM api_description ", fetch_all=True):
        if api_index not in api_detail:
            api_detail[api_index] = {}
        if api_name not in api_detail[api_index]:
            api_detail[api_index][api_name] = {}
        api_detail[api_index][api_name][en_name] = kr_name
    return api_detail

def insert_into_mongo_with_api_result(mongo_collection,api_detail,mongo_db,headers,params):
    insert_cnt = 0
    for collcection_name in mongo_collection:
        logger.info(f"colleciton name : {collcection_name}")
        mongo_db.set_collection(collcection_name)
        for api_name in set(api_detail[collcection_name].keys()):
            url = "http://data-dbg.krx.co.kr/svc/apis/{collcection_name}/{api_name}".format(collcection_name=collcection_name, api_name=api_name)
            logger.info(f"\turl : {url}")
            response = requests.get(url, headers=headers, params=params)
            result = response.json()
            result = mongo_db.get_collection().insert_many(result['OutBlock_1'])
            insert_cnt += len(result.inserted_ids)
            logger.info(f"\tinsert count : {len(result.inserted_ids)}")
    return insert_cnt



if __name__ == "__main__":
    #airflow에서 지정된 시간에 trigger 됨 --> 장이 끝나는 시간 + 평균적으로 데이터가 업데이트 되는 시간(대충 장 마감 시간 인듯) : trigger at 20:00
    #api 문서 : http://openapi.krx.co.kr/contents/OPP/USES/service/OPPUSES003_S2.cmd?BO_ID=nrEpCLaZpoLCTzPUMxuF
    #2010년 이후 데이터만 존재
    # 1. postgresql connection : mongo db 관련 collection 데이터 수집
    # 2. api data 수집
    # 3. end
    date = datetime.datetime.now()- datetime.timedelta(days=1)
    date_str = date.strftime('%Y%m%d')

    logger.info("marcap data pulling start")

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--before_date', type=str, default=date_str,
                            help="before date ")
    args = arg_parser.parse_args()
    logger.info(f"arg : {args.before_date}" )

    logger.info("get postgre information ")
    krx_api_db =  get_vault_configuration('krx_api_db')

    logger.info("get postgre singleton instance ")
    postgres = PostgresSingleton(krx_api_db)

    logger.info("get mongodb collection name ")
    mongo_collection = get_mongo_collection(postgres)

    logger.info("get api description name ")
    api_description = get_mongo_collection(postgres)

    logger.info("get mongodb singleton instance ")
    mongo = MongoDBSingleton.getInstance("krx_api_db")
    api_description = get_api_description(postgres)

    logger.info("set headers and params")
    headers = {
        "AUTH_KEY":  get_vault_configuration('krx_api')['token']
    }
    params = {
        "basDd": args.before_date  # 조회하고자 하는 날짜 (YYYYMMDD 형식)
    }
    logger.info("insert into mongodb using api")
    result = insert_into_mongo_with_api_result(mongo_collection,api_description,mongo,headers,params)

    logger.info(f"insert document count : {result}")

