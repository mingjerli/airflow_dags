from airflow import DAG
import datetime
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2023, 8, 3),
}

with DAG(
    dag_id="fraud_detection_dashboard",
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
    catchup=False,
    tags=["fraud_detection"],
) as dag:
    task_drop_fraud_result_table = PostgresOperator(
        task_id="drop_fraud_result_table", sql="""DROP TABLE IF EXISTS fraud_results"""
    )
    sql = """
CREATE TABLE fraud_results AS
    SELECT fdo.isfraud , fdf.isflaggedfraud, count(1) as count
    FROM fraud_detection_outcome fdo
    INNER JOIN fraud_detection_features fdf
    ON fdo.index=fdf.index
    Group by fdo.isfraud, rollup(fdf.isflaggedfraud)
    ORDER by fdf.isflaggedfraud, fdo.isfraud
"""
    task_update_fraud_db = PostgresOperator(task_id="update_fraud_db", sql=sql)

    task_drop_fraud_result_table >> task_update_fraud_db
