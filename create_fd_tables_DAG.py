from __future__ import annotations

import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

vendor_data_url = "https://raw.githubusercontent.com/LineaLabs/platform-demo/main/fraud-detection/data/PS_20174392719_1491204439457_log_v2.csv"

with DAG(
    dag_id="fraud_detection_reset",
    start_date=datetime.datetime(2023, 8, 3),
    schedule="@once",
    catchup=False,
    tags=['fraud_detection'],
) as dag:
    drop_fd__features_staging_if_exists = PostgresOperator(
        task_id = "drop_fd__features_staging_if_exists",
        sql = """DROP TABLE IF EXISTS fraud_detection_features_staging;"""
    )

    drop_fd_outcome_staging_if_exists = PostgresOperator(
        task_id = "drop_fd_outcome_staging_if_exists",
        sql = """DROP TABLE IF EXISTS fraud_detection_outcome_staging;"""
    )

    
    drop_fd_features_if_exists = PostgresOperator(
        task_id = "drop_fd_features_if_exists",
        sql = """DROP TABLE IF EXISTS fraud_detection_features;"""
    )

    drop_fd_outcome_if_exists = PostgresOperator(
        task_id = "drop_fd_outcome_if_exists",
        sql = """DROP TABLE IF EXISTS fraud_detection_outcome;"""
    )

    create_fd_features_staging = PostgresOperator(
        task_id = "create_fd_features_staging",
        sql = """
          CREATE TABLE fraud_detection_features_staging (
            index int8 PRIMARY KEY,
            amount float8 NULL,
            sender_old_balance float8 NULL,
            sender_new_balance float8 NULL,
            receiver_old_balance float8 NULL,
            receiver_new_balance float8 NULL,
            origin_code float8 NULL,
            "type_CASH_OUT" int2 NULL,
            "type_DEBIT" int2 NULL,
            "type_PAYMENT" int2 NULL,
            "type_TRANSFER" int2 NULL,
              "state" varchar(2) NULL
          );"""
    )

    create_fd_features = PostgresOperator(
        task_id = "create_fd_features",
        sql = """
        CREATE TABLE fraud_detection_features (
          index int8 PRIMARY KEY,
          amount float8 NULL,
          sender_old_balance float8 NULL,
          sender_new_balance float8 NULL,
          receiver_old_balance float8 NULL,
          receiver_new_balance float8 NULL,
          origin_code float8 NULL,
          "type_CASH_OUT" int2 NULL,
          "type_DEBIT" int2 NULL,
          "type_PAYMENT" int2 NULL,
          "type_TRANSFER" int2 NULL,
            "state" varchar(2) NULL,
          "timestamp" timestamptz NULL,
          isflaggedfraud int8 NULL 
        );"""
    )

    create_fd_outcome_staging = PostgresOperator(
        task_id = "create_fd_outcome_staging",
        sql = """
        CREATE TABLE fraud_detection_outcome_staging (
          index int8,
          isfraud int8 NULL
        );"""
    )

    create_fd_outcome = PostgresOperator(
        task_id = "create_fd_outcome",
        sql = """
        CREATE TABLE fraud_detection_outcome (
          index int8,
          isfraud int8 NULL
        );"""
    )


    drop_fd__features_staging_if_exists >> create_fd_features_staging
    drop_fd_features_if_exists >> create_fd_features

    drop_fd_outcome_staging_if_exists >> create_fd_outcome_staging
    drop_fd_outcome_if_exists >> create_fd_outcome