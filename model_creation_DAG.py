import datetime
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pickle


@dag(
    start_date=datetime.datetime(2023, 8, 3),
    schedule=datetime.timedelta(days=1),
    catchup=False,
    tags=["fraud_detection"],
)
def fraud_detection_model_creation():
    @task
    def task_get_fraud_detection_data():
        hook = PostgresHook()
        columns = 'isfraud, amount, sender_old_balance, sender_new_balance, receiver_old_balance, receiver_new_balance, origin_code, "type_CASH_OUT", "type_DEBIT", "type_PAYMENT", "type_TRANSFER"'
        sql_query = f"SELECT {columns} FROM fraud_detection_features \
            INNER JOIN fraud_detection_outcome \
            ON fraud_detection_features.index = fraud_detection_outcome.index \
            ;"
        df = hook.get_pandas_df(sql_query)
        return df

    @task
    def task_clean_fraud_detection_data(data):
        from fraud_detection_pipeline_module import split_fraud_detection_data

        data = split_fraud_detection_data(data)
        return data

    @task
    def task_get_fraud_detection_model(data):
        from fraud_detection_pipeline_module import get_fraud_detection_model

        random_forest_classifier = get_fraud_detection_model(
            data["X_train"], data["y_train"]
        )

        model = pickle.dumps(random_forest_classifier)
        s3hook = S3Hook()
        bucket_name = "fraud-detection-pipeline-data"
        key = "random_forest_model"
        s3hook.load_bytes(model, key=key, bucket_name=bucket_name, replace=True)

    @task
    def task_get_fraud_detection_predictions(data):
        from fraud_detection_pipeline_module import get_fraud_detection_predictions

        s3hook = S3Hook()
        file_obj = s3hook.get_key(
            key="random_forest_model", bucket_name="fraud-detection-pipeline-data"
        )
        random_forest_classifier = pickle.loads(file_obj.get()["Body"].read())
        rfc_pred = get_fraud_detection_predictions(
            random_forest_classifier, data["X_test"]
        )
        return rfc_pred

    @task
    def task_get_fraud_detection_performance_report(data, rfc_pred):
        from fraud_detection_pipeline_module import (
            get_fraud_detection_performance_report,
        )

        report = get_fraud_detection_performance_report(rfc_pred, data["y_test"])

        report_output = pickle.dumps(report)
        s3hook = S3Hook()
        bucket_name = "fraud-detection-pipeline-data"
        key = "model_report"
        s3hook.load_bytes(report_output, key=key, bucket_name=bucket_name, replace=True)

    # [START main_flow]
    data_processing = task_get_fraud_detection_data()
    clean_fraud_detection_data = task_clean_fraud_detection_data(data_processing)
    random_forest_classifier = task_get_fraud_detection_model(
        clean_fraud_detection_data
    )
    get_fraud_detection_predictions_result = task_get_fraud_detection_predictions(
        clean_fraud_detection_data
    )
    get_fraud_detection_performance_report_result = (
        task_get_fraud_detection_performance_report(
            clean_fraud_detection_data, get_fraud_detection_predictions_result
        )
    )
    random_forest_classifier >> get_fraud_detection_predictions_result
    # [END main_flow]


# [START dag_invocation]
fraud_detection_model_creation()
# [END dag_invocation]
