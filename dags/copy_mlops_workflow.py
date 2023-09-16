import pendulum
from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator

default_args = {"owner": "mavencode", "start_date": pendulum.datetime(2023, 9, 5, tz="UTC"), "depends_on_past": False}


@dag(
    dag_id="copy_mlops_pipeline",
    default_args=default_args,
    description="MLOps Pipeline",
    schedule_interval=None,
    max_active_runs=1,
    tags=["mlops"],
)
def copy_mlops_pipeline():
    """
    Apache Airflow DAG for running a workflow to ingest data, preprocess data, train model and evaluate model
    """

    def data_ingestion_op(input_data):
        from components.data_ingestion import data_ingestion

        ingested_data = data_ingestion(input_data)
        return ingested_data

    def data_preprocessing_op(ingested_data):
        from components.data_preprocessing import data_preprocessing

        processed_data = data_preprocessing(ingested_data)
        return processed_data

    def model_training_op(processed_data):
        from components.model_training import model_training

        model_path = model_training(processed_data)
        return model_path

    def model_evaluation_op(model_path, processed_data):
        from components.model_evaluation import model_evaluation

        result = model_evaluation(model_path, processed_data)
        return result

    input_data = (
        "https://raw.githubusercontent.com/MavenCode/MLOpsTraining-Dec2022/master/data/telco/churn_modeling.csv"
    )
    ingested_data_task = PythonOperator(
        task_id="data_ingestion_op", python_callable=data_ingestion_op, op_args=[input_data], provide_context=True
    )
    preprocessed_data_task = PythonOperator(
        task_id="data_preprocessing_op",
        python_callable=data_preprocessing_op,
        op_args=[ingested_data_task.output],
        provide_context=True,
    )
    trained_model_task = PythonOperator(
        task_id="model_training_op",
        python_callable=model_training_op,
        op_args=[preprocessed_data_task.output],
        provide_context=True,
    )
    model_evaluation_result_task = PythonOperator(
        task_id="model_evaluation_op",
        python_callable=model_evaluation_op,
        op_args=[trained_model_task.output, preprocessed_data_task.output],
        provide_context=True,
    )
    ingested_data_task >> preprocessed_data_task >> trained_model_task >> model_evaluation_result_task


mlops_dag = mlops_workflow()
