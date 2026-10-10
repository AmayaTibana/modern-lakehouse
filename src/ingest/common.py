import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

STORAGE_MODE = os.getenv("STORAGE_MODE", "local")
S3_BUCKET = os.getenv("S3_BUCKET")
LOCAL_ROOT = Path(__file__).resolve().parents[2] / "data"


def today_partition() -> str:
    """Partition date (UTC) as YYYY-MM-DD. PARTITION_DT env var overrides it."""
    override = os.getenv("PARTITION_DT")
    if override:
        return override
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def write_json(source: str, filename: str, payload: dict) -> str:
    """
    Save raw JSON at raw/<source>/dt=<date>/<filename>.
    Local mode writes under ./data/, S3 mode writes to the S3 bucket.
    """
    key = f"raw/{source}/dt={today_partition()}/{filename}"
    body = json.dumps(payload)

    if STORAGE_MODE == "s3":
        import boto3

        boto3.client("s3").put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )
        return f"s3://{S3_BUCKET}/{key}"

    path = LOCAL_ROOT / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return str(path)