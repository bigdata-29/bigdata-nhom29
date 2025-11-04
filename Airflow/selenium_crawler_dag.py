from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="topcv_crawler_dag",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
    tags=["topcv", "selenium"],
) as dag:

    crawl_task = KubernetesPodOperator(
        task_id="selenium_crawl",
        name="selenium-crawl",
        namespace="airflow",
        image="hoanvdtd/airflow-selenium:latest",
        cmds=["python", "/opt/airflow/dags/crawler.py"],  # đường dẫn trong image
        get_logs=True,
        is_delete_operator_pod=True
    )
