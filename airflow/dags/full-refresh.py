import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.kubernetes.secret import Secret

# from airflow.models.baseoperator import chain
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import logging
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)
# from google.auth import compute_engine

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    '/secrets/keyfile.json')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/secrets/keyfile.json"
x = os.system("cat /secrets/keyfile.json")
logger.info(x)

# credentials = compute_engine.Credentials()
aws_secret = Secret(
    deploy_type='volume',
    # Path where we mount the secret as volume
    deploy_target='/etc/aws',
    # Name of Kubernetes Secret
    secret='aws-creds',
    # Key in the form of service account file name
    key='aws-creds.json')

gcp_secret = Secret(
    deploy_type="volume",
    deploy_target="etc/gcp/",
    secret="gcsfs-creds",
    key="keyfile.json",
)

# os.environ['AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT'] = f'google-cloud-platform://?extra__google_cloud_platform__key_secret_name={gcp_secret}'

default_args = {
    "owner": "airflow",
    "start_date": datetime.now(),
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=60),
   
}

with DAG(
    dag_id="full-refresh",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=["full-refresh"],
) as dag:
    CLUSTER_NAME = "gke-cluster"
    PROJECT = os.getenv("PROJECT")
    CLUSTER_REGION = os.getenv("CLUSTER_REGION")
    STAGING_BUCKET = os.getenv("STAGING_BUCKET")

    from_s3_to_gcs = GKEStartPodOperator(
        # The ID specified for the task.
        task_id="data-transfer-task",
        # Name of task you want to run, used to generate Pod ID.
        name="data-transfer-task",
        project_id=PROJECT,
        location=CLUSTER_REGION,  # type: ignore
        cluster_name=CLUSTER_NAME,
        cmds=["/bin/bash", "./extract_data.sh", "yellow"],
        namespace="default",
        image="eu.gcr.io/stella-luxury-taxi/transfer-pod1",
        secrets=[aws_secret],
        env_vars={"PROJECT": PROJECT, "STAGING_BUCKET": STAGING_BUCKET, "AWS_CREDS": "/etc/aws/aws-creds.json"
                 , "GOOGLE_APPLICATION_CREDENTIALS": "/etc/gcp/keyfile.json"},
    )

    from_s3_to_gcs
