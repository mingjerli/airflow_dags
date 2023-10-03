from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.pod import \
    KubernetesPodOperator

with DAG(
    dag_id="copy_example_kubernetes_operator", schedule=None, start_date=datetime(2021, 1, 1), tags=["example"]
) as dag:
    write_xcom = KubernetesPodOperator(
        namespace="default",
        image="alpine",
        cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
        name="write-xcom",
        do_xcom_push=True,
        in_cluster=True,
        task_id="write-xcom",
        get_logs=True,
        service_account_name="airflow-sa",
    )
    pod_task_xcom_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('write-xcom')[0] }}\"", task_id="pod_task_xcom_result"
    )
    write_xcom >> pod_task_xcom_result
    write_xcom_async = KubernetesPodOperator(
        task_id="kubernetes_write_xcom_task_async",
        namespace="default",
        image="alpine",
        cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
        name="write-xcom",
        do_xcom_push=True,
        in_cluster=True,
        get_logs=True,
        deferrable=True,
        service_account_name="airflow-sa",
    )
    pod_task_xcom_result_async = BashOperator(
        task_id="pod_task_xcom_result_async", bash_command="echo \"{{ task_instance.xcom_pull('write-xcom')[0] }}\""
    )
    write_xcom_async >> pod_task_xcom_result_async
