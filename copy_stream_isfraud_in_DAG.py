from __future__ import annotations

import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

vendor_data_url = "https://raw.githubusercontent.com/LineaLabs/platform-demo/main/fraud-detection/data/PS_20174392719_1491204439457_log_v2.csv"
conn_str = "postgresql://airflow:airflow@postgres:5432/airflow"
with DAG(
    dag_id="copy_fraud_detection_stream_fraud",
    start_date=datetime.datetime(2023, 8, 3),
    schedule=datetime.timedelta(days=1),
    catchup=False,
    tags=["fraud_detection"],
) as dag:
    append_fd_from_staging = PostgresOperator(
        task_id="append_fd_outcome_from_staging",
        sql="\n        INSERT INTO fraud_detection_outcome\n        SELECT *\n        FROM fraud_detection_outcome_staging;",
    )
    truncate_fd_staging = PostgresOperator(
        task_id="truncate_fd_outcome_staging_table", sql="TRUNCATE TABLE fraud_detection_outcome_staging;"
    )
    append_fd_from_staging >> truncate_fd_staging
