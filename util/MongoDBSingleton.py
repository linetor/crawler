from pymongo import MongoClient

import os
from urllib.parse import quote_plus

import requests


def get_vault_configuration(endpoint):
    vault_addr = os.environ.get("VAULT_ADDR")
    vault_token = os.environ.get("VAULT_TOKEN")
    endpoint = f"{vault_addr}/v1/kv/data/{endpoint}"

    # HTTP GET 요청을 통해 데이터를 가져옵니다.
    headers = {"X-Vault-Token": vault_token}
    response = requests.get(endpoint, headers=headers, timeout=10)

    if response.status_code == 200:
        data = response.json()
        return data['data']['data']

    else:
        # 에러 응답의 경우 예외를 발생시킵니다.
        response.raise_for_status()


def _build_mongo_uri():
    """Mongo 접속 URI를 kv/mongodb 전용 자격증명으로 만든다."""
    info = get_vault_configuration("mongodb")
    required = ("user", "password", "host")
    missing = [key for key in required if not info.get(key)]
    if missing:
        raise ValueError(f"kv/mongodb 필수 필드 누락: {', '.join(missing)}")
    user = quote_plus(str(info["user"]))
    password = quote_plus(str(info["password"]))
    host = str(info["host"])
    port = int(info.get("port", 27017))
    auth_source = quote_plus(str(info.get("authSource") or "admin"))
    return f"mongodb://{user}:{password}@{host}:{port}/?authSource={auth_source}"


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

            uri = _build_mongo_uri()

            MongoDBSingleton.__instance = self

            self.client = MongoClient(uri)
            self.db = self.client[database_name]  # 여기에 사용할 데이터베이스명을 넣어주세요.
            self.collection = None

    def set_collection(self, collection_name):
        """사용할 컬렉션 설정"""
        self.collection = self.db[collection_name]

    def get_collection(self):
        """설정된 컬렉션 반환"""
        return self.collection