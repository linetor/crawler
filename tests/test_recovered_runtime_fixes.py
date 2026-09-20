import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[1]


def load_mongo_module():
    pymongo = types.ModuleType("pymongo")
    pymongo.MongoClient = MagicMock()
    sys.modules["pymongo"] = pymongo
    spec = importlib.util.spec_from_file_location(
        "mongodb_singleton_under_test",
        ROOT / "util" / "MongoDBSingleton.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class RecoveredRuntimeFixTests(unittest.TestCase):
    def test_mongodb_uri_uses_only_dedicated_secret_and_encodes_credentials(self):
        module = load_mongo_module()
        requested = []

        def get_secret(endpoint):
            requested.append(endpoint)
            return {
                "user": "mongo user",
                "password": "p@ss/word",
                "host": "db.internal",
                "port": "27018",
                "authSource": "admin db",
            }

        module.get_vault_configuration = get_secret
        uri = module._build_mongo_uri()

        self.assertEqual(requested, ["mongodb"])
        self.assertEqual(
            uri,
            "mongodb://mongo+user:p%40ss%2Fword@db.internal:27018/?authSource=admin+db",
        )

    def test_mongodb_secret_failure_is_fail_closed_without_ssh_fallback(self):
        module = load_mongo_module()
        requested = []

        def get_secret(endpoint):
            requested.append(endpoint)
            raise RuntimeError("synthetic Vault failure")

        module.get_vault_configuration = get_secret
        with self.assertRaisesRegex(RuntimeError, "synthetic Vault failure"):
            module._build_mongo_uri()
        self.assertEqual(requested, ["mongodb"])

    def test_mongodb_secret_requires_all_fields(self):
        module = load_mongo_module()
        module.get_vault_configuration = lambda _endpoint: {"user": "mongo", "host": "db"}

        with self.assertRaisesRegex(ValueError, "password"):
            module._build_mongo_uri()


if __name__ == "__main__":
    unittest.main()
