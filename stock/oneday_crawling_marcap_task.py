import argparse
import datetime
import logging
import os
import sys

import pandas as pd
current_path = os.path.abspath(__file__)
sys.path.append("/".join(current_path.split("/")[:-1])+'/../util')
from MongoDBSingleton import MongoDBSingleton

logger = logging.getLogger(name='marcap data pulling')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('|%(asctime)s||%(name)s||%(levelname)s|%(message)s',datefmt='%Y-%m-%d %H:%M:%S') 
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def pull_data(year_str):
    today_df = pd.read_parquet(f"https://github.com/FinanceData/marcap/raw/master/data/marcap-{year_str}.parquet")
    # The csv.gz source left Date as a plain 'YYYY-MM-DD' string, but parquet
    # types it as datetime64, so concatenating it raised TypeError. Normalize
    # through to_datetime (accepts either dtype) and format back to the exact
    # same string, so _id keeps matching the documents already in MongoDB.
    date_key = pd.to_datetime(today_df['Date']).dt.strftime('%Y-%m-%d')
    today_df['_id'] = today_df['Code'].astype(str) + "_" + date_key
    today_df['filename'] = f"marcap-{year_str}.parquet"
    return today_df

def getting_data_from_mongo(year_str,mongo):
    # 문서 전체(20컬럼)를 가져오면 안 된다. 호출부 get_complement_data 는
    # mongo_df[[_id,Code]] 만 쓰는데, 전체를 끌어오면 rasp4 에서 394,707건
    # 적재에 10분 이상 걸려 SSH cmd_timeout(1800s)을 넘겼다. 2026-07-27 에
    # csv.gz -> parquet 로 옮기며 대상 문서가 늘어난 뒤 07-29 부터 매일 실패했다.
    # projection 으로 두 컬럼만 받으면 같은 데이터가 7.7초에 들어온다(실측).
    #
    # filename 은 확장자만 다르므로 정확 일치 두 개를 $in 으로 묶는다. 앵커 없는
    # $regex 와 결과는 같지만(2026년은 parquet 394,707건뿐, csv.gz 0건) 의도가
    # 분명하고 조금 더 빠르다. filename 인덱스는 없으므로 어느 쪽이든 스캔이다.
    cursor = mongo.get_collection().find(
        {"filename": {"$in": [f"marcap-{year_str}.csv.gz", f"marcap-{year_str}.parquet"]}},
        {"_id": 1, "Code": 1},
    )
    mongo_df = pd.DataFrame(cursor)
    cursor.close()

    return mongo_df

def get_complement_data(today_df,mongo_df):
    #return today_df[~today_df.isin(mongo_df)].dropna()
    if mongo_df.empty:
        return today_df
    complement_df = pd.merge(
        today_df,
        mongo_df[['_id','Code']].rename(columns={"Code":"check_code"}),
        on='_id',how='left'
    )
    complement_df = complement_df[complement_df['check_code'].isnull()][today_df.columns]
    return complement_df


def insert_into_mongo(complement_df,mongo):
    #collection = mongo.get_collection().find({"filename": f"marcap-{year_str}.csv.gz"})
    data_dict = complement_df.to_dict('records')
    if len(data_dict) == 0:
        return 0
    insert_cnt = mongo.get_collection().insert_many(data_dict)
    return len(insert_cnt.inserted_ids)


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
    arg_parser.add_argument('--date', type=str, help="target date in YYYYMMDD format")
    args = arg_parser.parse_args()

    target_year = args.date[:4] if args.date else args.current_year_str
    logger.info(f"arg : {target_year}" )

    logger.info("get mongodb connection start ")
    mongo = MongoDBSingleton.getInstance("FinanceData")
    mongo.set_collection('marcap')
    logger.info("get mongodb connection end ")

    logger.info("pulling data from github ")
    today_df = pull_data(target_year)

    logger.info("pulling data cnt : " +str(today_df.shape) )

    logger.info("pulling data from mongodb ")
    mongo_df = getting_data_from_mongo(target_year,mongo)
    logger.info("data from mongodb : " + str(mongo_df.shape))

    logger.info("checking insert data ")
    complement_df = get_complement_data (today_df, mongo_df)
    logger.info(str(complement_df.shape) + " will be inserted ")

    insert_cnt = insert_into_mongo(complement_df,mongo)
    logger.info("inserted count : " + str(insert_cnt))

    logger.info("marcap data pulling end")
