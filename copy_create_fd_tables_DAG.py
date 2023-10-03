from __future__ import annotations

import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

vendor_data_url = "https://raw.githubusercontent.com/LineaLabs/platform-demo/main/fraud-detection/data/PS_20174392719_1491204439457_log_v2.csv"
with DAG(
    dag_id="copy_fraud_detection_reset",
    start_date=datetime.datetime(2023, 8, 21),
    schedule="@once",
    catchup=False,
    tags=["fraud_detection"],
) as dag:
    drop_fd__features_staging_if_exists = PostgresOperator(
        task_id="drop_fd__features_staging_if_exists", sql="DROP TABLE IF EXISTS fraud_detection_features_staging;"
    )
    drop_fd_outcome_staging_if_exists = PostgresOperator(
        task_id="drop_fd_outcome_staging_if_exists", sql="DROP TABLE IF EXISTS fraud_detection_outcome_staging;"
    )
    drop_fd_features_if_exists = PostgresOperator(
        task_id="drop_fd_features_if_exists", sql="DROP TABLE IF EXISTS fraud_detection_features;"
    )
    drop_fd_outcome_if_exists = PostgresOperator(
        task_id="drop_fd_outcome_if_exists", sql="DROP TABLE IF EXISTS fraud_detection_outcome;"
    )
    create_fd_features_staging = PostgresOperator(
        task_id="create_fd_features_staging",
        sql='\n          CREATE TABLE fraud_detection_features_staging (\n            index int8 PRIMARY KEY,\n            amount float8 NULL,\n            sender_old_balance float8 NULL,\n            sender_new_balance float8 NULL,\n            receiver_old_balance float8 NULL,\n            receiver_new_balance float8 NULL,\n            origin_code float8 NULL,\n            "type_CASH_OUT" int2 NULL,\n            "type_DEBIT" int2 NULL,\n            "type_PAYMENT" int2 NULL,\n            "type_TRANSFER" int2 NULL,\n              "state" varchar(2) NULL\n          );',
    )
    create_fd_features = PostgresOperator(
        task_id="create_fd_features",
        sql='\n        CREATE TABLE fraud_detection_features (\n          index int8 PRIMARY KEY,\n          amount float8 NULL,\n          sender_old_balance float8 NULL,\n          sender_new_balance float8 NULL,\n          receiver_old_balance float8 NULL,\n          receiver_new_balance float8 NULL,\n          origin_code float8 NULL,\n          "type_CASH_OUT" int2 NULL,\n          "type_DEBIT" int2 NULL,\n          "type_PAYMENT" int2 NULL,\n          "type_TRANSFER" int2 NULL,\n            "state" varchar(2) NULL,\n          "timestamp" timestamptz NULL,\n          isflaggedfraud int8 NULL \n        );',
    )
    create_fd_outcome_staging = PostgresOperator(
        task_id="create_fd_outcome_staging",
        sql="\n        CREATE TABLE fraud_detection_outcome_staging (\n          index int8,\n          isfraud int8 NULL\n        );",
    )
    create_fd_outcome = PostgresOperator(
        task_id="create_fd_outcome",
        sql="\n        CREATE TABLE fraud_detection_outcome (\n          index int8,\n          isfraud int8 NULL\n        );",
    )
    drop_fd__features_staging_if_exists >> create_fd_features_staging
    drop_fd_features_if_exists >> create_fd_features
    drop_fd_outcome_staging_if_exists >> create_fd_outcome_staging
    drop_fd_outcome_if_exists >> create_fd_outcome
