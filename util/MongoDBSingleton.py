from pymongo import MongoClient

import os
import requests
def get_vault_configuration(endpoint):
    vault_addr = os.environ.get("VAULT_ADDR")
    vault_token = os.environ.get("VAULT_TOKEN")
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

class MongoDBSingleton:
    __instance = None

    @staticmethod
    def getInstance(database_name):
        """싱글톤 인스턴스 반환"""
        if MongoDBSingleton.__instance is None:
            MongoDBSingleton(database_name)
        return MongoDBSingleton.__instance

    def __init__(self,database_name):
        """MongoDBSingleton 생성자"""
        if MongoDBSingleton.__instance is not None:
            raise Exception("싱글톤 클래스입니다. 사용하세요.")
        else:

            ssh_info = get_vault_configuration("ssh")
            ssh_ip = ssh_info['ssh_ip']['odroid']
            id = ssh_info['ssh_id']
            passwd = ssh_info['ssh_pass']

            MongoDBSingleton.__instance = self

            self.client = MongoClient(f'mongodb://{id}:{passwd}@{ssh_ip}:27017/')  # 여기에 MongoDB 접속 정보를 넣어주세요.
            self.db = self.client['database_name']  # 여기에 사용할 데이터베이스명을 넣어주세요.
            self.collection = None

    def set_collection(self, collection_name):
        """사용할 컬렉션 설정"""
        self.collection = self.db[collection_name]

    def get_collection(self):
        """설정된 컬렉션 반환"""
        return self.collection