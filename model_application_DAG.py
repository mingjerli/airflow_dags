from airflow.decorators import dag, task
import datetime
import pickle
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def read_airflow_query(sql_query):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pshook = PostgresHook()
    df = pshook.get_pandas_df(sql_query)
    return df


def read_s3_pickle_file(s3_key, s3_bucket):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import pickle

    s3hook = S3Hook()
    file_obj = s3hook.get_key(key=s3_key, bucket_name=s3_bucket)
    return pickle.loads(file_obj.get()["Body"].read())


def update_sql_table(sql_query, data):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pshook = PostgresHook()
    conn = pshook.get_conn()
    cursor = conn.cursor()
    cursor.executemany(sql_query, data)
    conn.commit()
    conn.close()
    ## If we use the following code instead previous five lines of code, the result should always None since the update is not committed.
    # pshook.get_conn().cursor().executemany(sql, data)


@dag(
    start_date=datetime.datetime(2023, 8, 4),
    schedule=datetime.timedelta(minutes=10),
    catchup=False,
    tags=["fraud_detection"],
)
def fraud_detection_model_application():
    @task
    def task_get_fraud_detection_data():
        columns = 'index, amount, sender_old_balance, sender_new_balance, receiver_old_balance, receiver_new_balance, origin_code, "type_CASH_OUT", "type_DEBIT", "type_PAYMENT", "type_TRANSFER"'
        sql_query = f"SELECT {columns} FROM fraud_detection_features;"
        df = read_airflow_query(sql_query)
        data = df.drop(columns=["index"], axis="columns")
        index = df[["index"]]
        return {"data": data, "index": index}

    @task
    def task_upload_fraud_detection_model_result(inputs):
        data = inputs["data"]
        index = inputs["index"]
        if data.shape[0] > 0:
            random_forest_classifier = read_s3_pickle_file(
                s3_key="random_forest_model", s3_bucket="fraud-detection-pipeline-data"
            )

            pred = random_forest_classifier.predict_log_proba(data.values)[:, 0]
            data = [
                (int(pred[i]), int(idx)) for i, idx in enumerate(index["index"].values)
            ]
            sql = "UPDATE fraud_detection_features SET isflaggedfraud = %s WHERE index = %s;"

            update_sql_table(sql, data)

    # [START main_flow]
    get_data = task_get_fraud_detection_data()
    upload_results = task_upload_fraud_detection_model_result(get_data)
    # [END main_flow]


# [START dag_invocation]
fraud_detection_model_application()
# [END dag_invocation]
