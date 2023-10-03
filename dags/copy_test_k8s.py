import pendulum
from airflow.decorators import dag, task

default_args = {"owner": "randomguys", "start_date": pendulum.datetime(2023, 9, 5, tz="UTC"), "depends_on_past": False}
mlops_base_image = "mingjerli/mlops_base_image"


@dag(
    dag_id="copy_test_k8s",
    default_args=default_args,
    description="MLOps Pipeline K8S",
    schedule_interval=None,
    max_active_runs=1,
    tags=["mlops"],
)
def mlops_workflow():
    """
    Apache Airflow DAG for running a workflow to ingest data, preprocess data, train model and evaluate model
    """

    @task.kubernetes(
        image=mlops_base_image,
        task_id="data_ingestion_op",
        namespace="airflow",
        in_cluster=True,
        get_logs=True,
        do_xcom_push=True,
        startup_timeout_seconds=300,
        service_account_name="airflow-sa",
    )
    def data_ingestion_op(input_data):
        from components.data_ingestion import data_ingestion

        data_ingestion(input_data)

    input_data = "https://raw.githubusercontent.com/sharmaroshan/Churn-Modelling-Dataset/master/Churn_Modelling.csv"
    data_ingestion_op(input_data)


mlops_workflow()
