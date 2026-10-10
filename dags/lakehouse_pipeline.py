import sys
from datetime import datetime, timedelta

sys.path.append("/usr/local/airflow")

from airflow.sdk import dag, task  # noqa: E402


@dag(
    dag_id="lakehouse_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse"],
)
def lakehouse_pipeline():
    @task
    def ingest_pokeapi():
        from src.ingest.ingest_pokeapi import run

        return run()

    @task
    def ingest_weather():
        from src.ingest.ingest_weather import run

        return run()

    ingest_pokeapi()
    ingest_weather()


lakehouse_pipeline()