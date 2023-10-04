import pendulum
from airflow.decorators import dag, task

default_args = {"owner": "randomguys", "start_date": pendulum.datetime(2023, 9, 5, tz="UTC"), "depends_on_past": False}
mlops_base_image = "mingjerli/mlops_base_image"


@dag(
    dag_id="copy_naive_k8s",
    default_args=default_args,
    description="MLOps Pipeline K8S",
    schedule_interval=None,
    max_active_runs=1,
    tags=["test"],
)
def mlops_workflow():
    @task.kubernetes(
        image=mlops_base_image,
        task_id="step1",
        namespace="airflow",
        in_cluster=True,
        get_logs=True,
        do_xcom_push=True,
        startup_timeout_seconds=300,
        service_account_name="airflow-sa",
    )
    def data_ingestion_op(input_data):
        import datetime

        from components.data_ingestion import data_ingestion

        ingested_data = data_ingestion(input_data)
        ingested_data.to_json("/airflow/xcom/return.json")
        ingested_data = ingested_data.to_dict("records")
        current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return current_timestamp

    @task.kubernetes(
        image=mlops_base_image,
        task_id="data_preprocessing_op",
        namespace="airflow",
        in_cluster=True,
        get_logs=True,
        do_xcom_push=True,
        startup_timeout_seconds=300,
        service_account_name="airflow-sa",
    )
    def data_preprocessing_op(current_timestamp):
        return current_timestamp * 10

    input_data = "https://raw.githubusercontent.com/sharmaroshan/Churn-Modelling-Dataset/master/Churn_Modelling.csv"
    current_timestamp = data_ingestion_op(input_data)
    data_preprocessing_op(current_timestamp)


mlops_workflow()
