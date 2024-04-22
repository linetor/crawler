import subprocess
import datetime
import argparse
import sys
from configparser import ConfigParser
import time
import logging
import pandas as pd

import sys
sys.path.append('../util')
from MongoDBSingleton import MongoDBSingleton

logger = logging.getLogger(name='marcap data pulling')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('|%(asctime)s||%(name)s||%(levelname)s|%(message)s',datefmt='%Y-%m-%d %H:%M:%S') 
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def pull_data(year_str):
    today_df = pd.read_csv(f"https://github.com/FinanceData/marcap/raw/master/data/marcap-{year_str}.csv.gz")
    today_df['_id'] = today_df['Code'].astype(str)+"_"+today_df['Date']
    today_df['filename'] = f"marcap-{year_str}.csv.gz"
    return today_df

def getting_data_from_mongo(year_str,mongo):
    cursor = mongo.get_collection().find({"filename": f"marcap-{year_str}.csv.gz"})
    mongo_df = pd.DataFrame(cursor)
    cursor.close()

    return mongo_df

def get_complement_data(today_df,mongo_df):
    return today_df[~today_df.isin(mongo_df)].dropna()

def insert_into_mongo(complement_df,mongo):
    #collection = mongo.get_collection().find({"filename": f"marcap-{year_str}.csv.gz"})
    data_dict = complement_df.to_dict('records')
    insert_cnt = mongo.get_collection().insert_many(data_dict)

    return insert_cnt


if __name__ == "__main__":
    #airflow에서 지정된 시간에 trigger 됨 --> 장이 끝나는 시간 + 평균적으로 데이터가 업데이트 되는 시간(대충 장 마감 시간 인듯) : trigger at 17:00
    #데이터 장소 : https://github.com/FinanceData/marcap/blob/master/data/marcap-2024.csv.gz
    # 1. pull data and making pandas data frame
    # 2. getting mongodb data
    # 3. extract data 1-2
    # 4. insert into mongodb with 3 data
    # 5. TODO : extract mongo-data into redis for dashboard
    date = datetime.datetime.now()
    year_str = date.strftime('%Y')

    logger.info("marcap data pulling start")

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--current_year_str', type=str, default=year_str,
                            help="current year ")
    args = arg_parser.parse_args()
    logger.info(f"arg : {{args.start_time_str}}" )

    logger.info("get mongodb connection start ")
    mongo = MongoDBSingleton.getInstance("FinanceData")
    mongo.set_collection('marcap')
    logger.info("get mongodb connection end ")

    logger.info("pulling data from github ")
    today_df = pull_data(args.current_year_str)

    logger.info("pulling data cnt : " +str(today_df.shape) )

    logger.info("pulling data from mongodb ")
    mongo_df = getting_data_from_mongo(args.current_year_str,mongo)

    logger.info("checking insert data ")
    complement_df = get_complement_data (today_df, mongo_df)
    logger.info(str(complement_df.shape) + " will be inserted ")

    insert_cnt = insert_into_mongo(complement_df,mongo)
    logger.info("inserted count : " + str(len(insert_cnt)))

    logger.info("marcap data pulling end")
