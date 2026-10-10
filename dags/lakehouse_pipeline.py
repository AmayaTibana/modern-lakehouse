import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

sys.path.append("/usr/local/airflow")

from airflow.sdk import dag, task

PROJECT_DIR = "/usr/local/airflow"


@dag(
    dag_id="lakehouse_pipeline",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse"],
)
def lakehouse_pipeline():
    @task
    def run_date() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    @task
    def ingest_pokeapi(dt: str):
        os.environ["PARTITION_DT"] = dt
        from src.ingest.ingest_pokeapi import run

        return run()

    @task
    def ingest_weather(dt: str):
        os.environ["PARTITION_DT"] = dt
        from src.ingest.ingest_weather import run

        return run()

    @task
    def spark_transform(dt: str):
        subprocess.run(
            [sys.executable, "-m", "src.spark_jobs.transform", "--dt", dt],
            cwd=PROJECT_DIR,
            check=True,
        )

    @task
    def load_snowflake():
        from src.load.load_snowflake import run

        run()

    @task
    def quality_checks():
        from src.quality.checks import run

        run()

    dt = run_date()
    pokeapi = ingest_pokeapi(dt)
    weather = ingest_weather(dt)
    transform = spark_transform(dt)

    [pokeapi, weather] >> transform >> load_snowflake() >> quality_checks()


lakehouse_pipeline()