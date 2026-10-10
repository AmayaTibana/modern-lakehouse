import os
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

load_dotenv()

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def _load_private_key() -> bytes:
    key_path = os.path.expanduser(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"])
    with open(key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(key_file.read(), password=None)
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=_load_private_key(),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "LAKEHOUSE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "LAKEHOUSE"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def run(files=("03_load_staging.sql", "04_star_schema.sql")) -> None:
    conn = connect()
    try:
        for name in files:
            sql = (SQL_DIR / name).read_text(encoding="utf-8")
            for _ in conn.execute_string(sql):
                pass
            print(f"Ran {name}")
    finally:
        conn.close()


if __name__ == "__main__":
    run()