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

def _build_mongo_uri():
    """Mongo 접속 URI 를 만든다. kv/mongodb 를 우선 사용한다.

    2026-08-12: 예전에는 kv/ssh 의 SSH 계정(ssh_id/ssh_pass)을 그대로 Mongo
    자격증명으로 썼다. Mongo 비밀번호가 kv/mongodb 로 분리되면서 kv/ssh 의
    값은 더 이상 Mongo 에 통하지 않게 됐고, run_marcap_update_scrip 이
    "Authentication failed (code 18)" 로 매일 실패했다.

    실측(2026-08-12): kv/mongodb 자격 -> 인증 성공, kv/ssh 자격 -> 인증 실패.

    kv/mongodb 를 읽지 못하는 환경을 위해 예전 경로를 fallback 으로 남긴다.
    다만 fallback 은 조용히 넘어가지 않고 이유를 출력한다. 폴백이 조용하면
    "왜 옛 비밀번호를 쓰는지" 를 알 수 없게 된다.
    """
    try:
        info = get_vault_configuration("mongodb")
        user = info["user"]
        passwd = info["password"]
        host = info["host"]
        port = int(info.get("port", 27017))
        auth_source = info.get("authSource") or "admin"
        return f"mongodb://{user}:{passwd}@{host}:{port}/?authSource={auth_source}"
    except Exception as exc:
        print(f"[MongoDBSingleton] kv/mongodb 조회 실패({exc}); kv/ssh 로 폴백합니다.")
        ssh_info = get_vault_configuration("ssh")
        return (
            f"mongodb://{ssh_info['ssh_id']}:{ssh_info['ssh_pass']}"
            f"@{ssh_info['ssh_ip']['odroid']}:27017/"
        )


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