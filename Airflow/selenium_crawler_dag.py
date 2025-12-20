from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta

# Cấu hình chung cho các Task chạy trên K8s
K8S_CONFIG = {
    "namespace": "airflow",
    "image": "hoanvdtd/airflow-selenium:latest",
    "image_pull_policy": "IfNotPresent",
    "in_cluster": True,
    "get_logs": True,
    "is_delete_operator_pod": True,
    "startup_timeout_seconds": 300,
}

# Biến môi trường
SPARK_ENV_VARS = {
    "HADOOP_USER_NAME": "airflow",
    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
    # "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop" # Optional
}

with DAG(
    dag_id="topcv_crawler_dag",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
    tags=["topcv", "selenium", "spark", "hdfs"],
) as dag:

    # ---------------------------------------------------------
    # TASK 1: CRAWL DATA
    # ---------------------------------------------------------
    # Crawler tự lo việc upload lên HDFS qua WebHDFS (port 9870)
    # nên ta chỉ cần gọi lệnh python chạy script.
    crawl_task = KubernetesPodOperator(
        task_id="selenium_crawl",
        name="selenium-crawl",
        cmds=["python", "/opt/airflow/dags/crawler.py"], 
        env_vars=SPARK_ENV_VARS,
        **K8S_CONFIG
    )

    # ---------------------------------------------------------
    # TASK 2: PRE-PROCESSING (Spark Job 1)
    # ---------------------------------------------------------
    preprocess_task = KubernetesPodOperator(
        task_id="spark_preprocessing",
        name="spark-preprocessing",
        cmds=["python", "/opt/airflow/dags/spark_preprocessing_job1.py"],
        env_vars=SPARK_ENV_VARS,
        **K8S_CONFIG
    )

    # ---------------------------------------------------------
    # TASK 3: CLEANING & LOAD DW (Spark Job 2)
    # ---------------------------------------------------------
    clean_task = KubernetesPodOperator(
        task_id="spark_cleaning",
        name="spark-cleaning",
        cmds=["python", "/opt/airflow/dags/spark_cleaning_job1.py"],
        env_vars=SPARK_ENV_VARS,
        **K8S_CONFIG
    )

    # ---------------------------------------------------------
    # LUỒNG CHẠY
    # ---------------------------------------------------------
    crawl_task >> preprocess_task >> clean_task