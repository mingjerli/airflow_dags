import pendulum
from airflow.decorators import dag, task

default_args = {"owner": "randomguys", "start_date": pendulum.datetime(2023, 9, 5, tz="UTC"), "depends_on_past": False}
mlops_base_image = "mingjerli/mlops_base_image"


@dag(
    dag_id="""copy_mlops_pipeline_k8s""",
    default_args=default_args,
    description="""MLOps Pipeline K8S""",
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
        task_id="""data_ingestion_op""",
        namespace="""airflow""",
        in_cluster=True,
        get_logs=True,
        do_xcom_push=True,
        startup_timeout_seconds=300,
        service_account_name="""airflow-sa""",
    )
    def data_ingestion_op(input_data):
        from components.data_ingestion import data_ingestion

        ingested_data = data_ingestion(input_data)
        return ingested_data

    @task.kubernetes(
        image=mlops_base_image,
        task_id="""data_preprocessing_op""",
        namespace="""airflow""",
        in_cluster=True,
        get_logs=True,
        do_xcom_push=True,
        startup_timeout_seconds=300,
        service_account_name="""airflow-sa""",
    )
    def data_preprocessing_op(ingested_data):
        from components.data_preprocessing import data_preprocessing

        processed_data = data_preprocessing(ingested_data)
        return processed_data

    @task.kubernetes(
        image=mlops_base_image,
        task_id="""model_training_op""",
        namespace="""airflow""",
        in_cluster=True,
        get_logs=True,
        do_xcom_push=True,
        startup_timeout_seconds=300,
        service_account_name="""airflow-sa""",
    )
    def model_training_op(processed_data):
        from components.model_training import model_training

        model_path = model_training(processed_data)
        return model_path

    @task.kubernetes(
        image=mlops_base_image,
        task_id="""model_evaluation_op""",
        namespace="""airflow""",
        in_cluster=True,
        get_logs=True,
        do_xcom_push=True,
        startup_timeout_seconds=300,
        service_account_name="""airflow-sa""",
    )
    def model_evaluation_op(model_path, processed_data):
        from components.model_evaluation import model_evaluation

        result = model_evaluation(model_path, processed_data)
        return result

    input_data = "https://raw.githubusercontent.com/sharmaroshan/Churn-Modelling-Dataset/master/Churn_Modelling.csv"
    ingested_data = data_ingestion_op(input_data)
    preprocessed_data = data_preprocessing_op(ingested_data)
    trained_model = model_training_op(preprocessed_data)
    model_evaluation_op(trained_model, preprocessed_data)


mlops_workflow()
