import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

default_args = {"owner": "randomguys", "start_date": pendulum.datetime(2023, 9, 5, tz="UTC"), "depends_on_past": False}
mlops_base_image = "mingjerli/mlops_base_image"


@dag(
    dag_id="mlops_pipeline_k8s_s3",
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
        env_vars={
            "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),
        },
        startup_timeout_seconds=300,
        service_account_name="airflow-sa",
    )
    def data_ingestion_op(input_data):
        from components.data_ingestion import data_ingestion
        from components.s3io import pickle_s3

        ingested_data = data_ingestion(input_data)
        ingested_data_path = "s3://mjlpvc/xcml/ingested_data.pkl"
        pickle_s3(ingested_data, ingested_data_path)
        return ingested_data_path

    @task.kubernetes(
        image=mlops_base_image,
        task_id="data_preprocessing_op",
        namespace="airflow",
        in_cluster=True,
        get_logs=True,
        do_xcom_push=True,
        env_vars={
            "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),
        },
        startup_timeout_seconds=300,
        service_account_name="airflow-sa",
    )
    def data_preprocessing_op(ingested_data_path):
        from components.data_preprocessing import data_preprocessing
        from components.s3io import pickle_s3, unpickle_s3

        ingested_data = unpickle_s3(ingested_data_path)
        processed_data = data_preprocessing(ingested_data)
        processed_data_path = "s3://mjlpvc/xcml/processed_data.pkl"
        pickle_s3(processed_data, processed_data_path)
        return processed_data_path

    @task.kubernetes(
        image=mlops_base_image,
        task_id="model_training_op",
        namespace="airflow",
        in_cluster=True,
        get_logs=True,
        do_xcom_push=True,
        env_vars={
            "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),
        },
        startup_timeout_seconds=300,
        service_account_name="airflow-sa",
    )
    def model_training_op(processed_data_path):
        from components.model_training import model_training
        from components.s3io import pickle_s3, unpickle_s3

        processed_data = unpickle_s3(processed_data_path)
        model = model_training(processed_data)
        model_path = "s3://mjlpvc/xcml/model.pkl"
        pickle_s3(model, model_path)
        return model_path

    @task.kubernetes(
        image=mlops_base_image,
        task_id="model_evaluation_op",
        namespace="airflow",
        in_cluster=True,
        get_logs=True,
        do_xcom_push=True,
        env_vars={
            "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),
        },
        startup_timeout_seconds=300,
        service_account_name="airflow-sa",
    )
    def model_evaluation_op(model_path, processed_data_path):
        from components.model_evaluation import direct_model_evaluation
        from components.s3io import pickle_s3, unpickle_s3

        model = unpickle_s3(model_path)
        processed_data = unpickle_s3(processed_data_path)
        result = direct_model_evaluation(model, processed_data)
        result_path = "s3://mjlpvc/xcml/result.pkl"
        pickle_s3(result, result_path)
        return result_path

    input_data = "https://raw.githubusercontent.com/sharmaroshan/Churn-Modelling-Dataset/master/Churn_Modelling.csv"
    ingested_data = data_ingestion_op(input_data)
    preprocessed_data = data_preprocessing_op(ingested_data)
    trained_model = model_training_op(preprocessed_data)
    model_evaluation_op(trained_model, preprocessed_data)


mlops_workflow()
