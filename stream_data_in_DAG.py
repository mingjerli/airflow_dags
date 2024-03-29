from __future__ import annotations

import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonVirtualenvOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
from sqlalchemy import create_engine
import us

vendor_data_url = "https://raw.githubusercontent.com/LineaLabs/platform-demo/main/fraud-detection/data/PS_20174392719_1491204439457_log_v2.csv"
conn_str = "postgresql+psycopg2://airflow:airflow@postgres/airflow"


def get_fraud_detection_raw_data():
    # engine = create_engine(conn_str)
    # sql = 'SELECT MAX("index") FROM fraud_detection_features'
    # last_index = pd.read_sql(sql, engine).iloc[0, 0]
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = 'SELECT MAX("index") FROM fraud_detection_features'
    last_index = pg_hook.get_first(sql)[0]
    if last_index == None:
        last_index = -1

    rawdata = pd.read_csv(vendor_data_url)
    data = rawdata.rename(
        columns={
            "nameOrig": "origin",
            "oldbalanceOrg": "sender_old_balance",
            "newbalanceOrig": "sender_new_balance",
            "nameDest": "destination",
            "oldbalanceDest": "receiver_old_balance",
            "newbalanceDest": "receiver_new_balance",
            "isFraud": "isfraud",
        }
    ).drop(columns=["step", "isFlaggedFraud"], axis="columns")
    cols = data.columns.tolist()
    new_position = 3
    cols.insert(new_position, cols.pop(cols.index("destination")))
    data = data[cols]
    data["origin_code"] = 0  # to prevent messing with get_dummies
    data.drop(columns=["origin", "destination"], axis="columns", inplace=True)
    data = pd.get_dummies(data, drop_first=True).sample(frac=0.1)
    statedata = pd.concat(
        [
            data.assign(state=state.abbr).sample(data.shape[0] // 5)
            for state in us.STATES
        ]
    )
    statedata = statedata.reset_index(drop=True)
    statedata = statedata.reset_index()
    statedata["index"] = statedata["index"] + last_index + 1
    featuresdata = statedata.drop(columns=["isfraud"], axis="columns")
    featuresdata = featuresdata.astype(
        {c: "int32" for c in featuresdata.columns if c.startswith("type_")}
    )
    # featuresdata.to_csv("/opt/airflow/data/test.csv")
    # featuresdata.to_sql(
    #     "fraud_detection_features_staging", engine, if_exists="append", index=False
    # )
    # statedata[["index", "isfraud"]].to_sql(
    #     "fraud_detection_outcome_staging", engine, if_exists="append", index=False
    # )
    records = featuresdata.to_records(index=False).tolist()
    pg_hook.insert_rows("fraud_detection_features_staging", records)
    outcome = statedata[["index", "isfraud"]]
    records = outcome.to_records(index=False).tolist()
    pg_hook.insert_rows("fraud_detection_outcome_staging", records)


with DAG(
    dag_id="fraud_detection_stream_data",
    start_date=datetime.datetime(2023, 8, 3),
    schedule=datetime.timedelta(hours=1),
    catchup=False,
    tags=["fraud_detection"],
) as dag:
    truncate_fd_staging = PostgresOperator(
        task_id="truncate_fd_staging_table",
        sql="""TRUNCATE TABLE fraud_detection_features_staging;""",
    )

    ingest_fd_staging = PythonOperator(
        task_id="ingest_fd_staging",
        python_callable=get_fraud_detection_raw_data,
    )

    # ingest_fd_staging = PythonVirtualenvOperator(
    #     task_id="ingest_fd_staging",
    #     requirements=[
    #         "scikit-learn==1.0.2",
    #         "pandas==1.3.3",
    #         "numpy==1.24.2",
    #         "us",
    #         "sqlalchemy==1.4.49",
    #         "psycopg2-binary",
    #     ],
    #     pip_install_options=["--no-cache-dir", "--force-reinstall"],
    #     system_site_packages=False,
    #     python_callable=get_fraud_detection_raw_data,
    # )

    append_fd_from_staging = PostgresOperator(
        task_id="append_fd_from_staging",
        sql="""
        INSERT INTO fraud_detection_features
        SELECT *, CURRENT_TIMESTAMP as timestamp, NULL
        FROM fraud_detection_features_staging;""",
    )

    truncate_fd_staging >> ingest_fd_staging >> append_fd_from_staging
