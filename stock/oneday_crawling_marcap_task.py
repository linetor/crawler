import subprocess
import datetime
import argparse
import sys
from configparser import ConfigParser
import time
import logging

logger = logging.getLogger(name='marcap data pulling')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('|%(asctime)s||%(name)s||%(levelname)s|%(message)s',datefmt='%Y-%m-%d %H:%M:%S') 
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

def pull_data():
    return None

def getting_data_from_mongo():
    return None

def get_complement_data():
    return None

def insert_into_mongo():
    return None


if __name__ == "__main__":
    #airflow에서 지정된 시간에 trigger 됨 --> 장이 끝나는 시간 + 평균적으로 데이터가 업데이트 되는 시간(대충 장 마감 시간 인듯) : trigger at 17:00
    #데이터 장소 : https://github.com/FinanceData/marcap/blob/master/data/marcap-2024.csv.gz
    # 1. pull data and making pandas data frame
    # 2. getting mongodb data
    # 3. extract data 1-2
    # 4. insert into mongodb with 3 data
    # 5. TODO : extract mongo-data into redis for dashboard
    logger.info("marcap data pulling starting")