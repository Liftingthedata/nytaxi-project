import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# from airflow.models.baseoperator import chain
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import logging
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)
# from google.auth import compute_engine

  
# from google.oauth2 import service_account
# from googleapiclient import discovery

# #Downloaded credentials in JSON format
# gcp_sa_credentials={
#   "type": "service_account",
#   "project_id": "stella-luxury-taxi",
#   "private_key_id": "e6897c764237693277647217f46edcc971d6fa03",
#   "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCp7TdAB7pjalXw\nlrv69Jo+oOUljranFpqJNiGcQ3NJEhEbN7XFDBG8unn+vZwPwE0cfTk11C/B2WLR\nZwzJf4ivQ7IqlNKgYWbQJGpFCwDX1YnSNFbZyNddMyg0FuEvuQ//xSBTfrhb6Vl2\nQ9YQLovFHn40XTs+NsTLQZdeknOEdsA8aMR74/Q71EJP+RnMJ8uZnWIIpEVn1TyE\n/fLOBePTUMwgEtPkhsHWycyW4u9DQi7wcp2zNA+1nBb7I34laAf/cLFdLU3MSDuH\nz/nMHpTHzEbSaIpjYqFOQfgGEUn86g46/YoA8/J3/cOKG0WVXIFNJ5bVST1zgEHP\nIM2gOq77AgMBAAECggEACs6TXPcg24G/xXVQrz0vMhx5dlIFO8ssON3AdXe0tUj4\n3YpFfSqvVhll+NWGP1ozjURJhyrffqycpESxg38g6kSb6Cle2+RV7ZbjS1DP1Oo3\nwa6id2dWiw7d17I80BQs+E9JJwZAI1hL4EGgM5dCPF8cF6h2RBannWWmgtU9k4b/\nGz2dznZoQMVN/q09zYW/jeQbvkvldSfvuuJH3ucMPwGXVZdEPpz9i5ut9tc/af6J\nn0H8f67PMD8X3kkDCYj4IL1KCNEY7+ftoY7+Imd3FRH6uPEw17FkEYFqFb/FIxRi\nm8lYj/MjswBdHZCuyqMIFI+xgQXPkFf/dzXOAyLAwQKBgQDvQF8lPUOZ7GSx+Dni\n2Ik+7OGkJC8cR29Qbv4Wsz11bzqNTvNajdGTUQW2KF3L84D7qsLVvgbfAmczFMDx\nGeV8VdXhNkLjxsCLqYxU7S+gN8ifCZwVCMPSEa9kDdir52UZzLwH1sHYzd4LPdra\nKl+WMo0VeTXt15Sr3z71Q713tQKBgQC10nkY2iErMKFEbajUBJlS9jZ7aSXSFzjo\neSGYOVBYZi9oJD/KizUsB2kWxK4HOkKyq6sgyrdVHm3hmqk52SUVoWkD3IcNF6hE\nqOm+Vf2a2yColnB2xhd0KBcGgi4fIy8buqpkmn2Lxl+XAyx/XCKWH4fzzZ9QNrn4\nQhuBVRJZ7wKBgQDSbCNCdWeHcUn+3PrMcPYEygKKguiMTqewbm47ONnM907gCZgv\nBJxWnOQRGd+lCT1gGwfRRZh1e3+YhaBMbSJRAI1jzn12J9AhBbXO2+0PVQC2H5WP\nSm4vzC4eKa9vQczBrDeUDWXgcO/hoz1gs/Pt/ffn8vtjfD/eCjMtM67oIQKBgCSK\ndyHifLYEYPSyoTJy9ilxKAPnXt15I0u9RF4mbppFdxOT7WoUTgxaNOmJf3weXlcw\nHwVJGE03/1dO0OG6XTSaqtNG17Fu5rddxxQkjgI4NbkL+vAz4XTLtczuDrzdQlNt\nUV9EmSSlKoLb9W5nIuBO1/DMi08AoKFfD84PPc/rAoGBALBmwjsygh4HbO1hrbdT\n2Q44LZpYL0PRCMLVRGtaBh10/TinQuie+pMa+sqrpNb0T9NnE6n3NN9ewDbTZ279\njkuOQVpoHZPWGwGC0ki9aupDK7Sr5RcUT9vMyN5GtSky8DwkKrRPUX/3QIR9LSDA\n9rUdxCeXAYlsYkK4m8mDJUIz\n-----END PRIVATE KEY-----\n",
#   "client_email": "gke-sa@stella-luxury-taxi.iam.gserviceaccount.com",
#   "client_id": "106181116653793762876",
#   "auth_uri": "https://accounts.google.com/o/oauth2/auth",
#   "token_uri": "https://oauth2.googleapis.com/token",
#   "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
#   "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/gke-sa%40stella-luxury-taxi.iam.gserviceaccount.com"
# }

# project_id=gcp_sa_credentials["project_id"]

# credentials = service_account.Credentials.from_service_account_info(gcp_sa_credentials)

    
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


# aws_secret = Secret(
#     deploy_type='volume',
#     # Path where we mount the secret as volume
#     deploy_target='/etc/aws',
#     # Name of Kubernetes Secret
#     secret='aws-creds',
#     # Key in the form of service account file name
#     key='aws-creds.json')

# gcp_secret = Secret(
#     deploy_type="volume",
#     deploy_target="etc/gcp/",
#     secret="gcsfs-creds",
#     key="keyfile.json",
# )

# # os.environ['AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT'] = f'google-cloud-platform://?extra__google_cloud_platform__key_secret_name={gcp_secret}'

default_args = {
    "owner": "airflow",
    "start_date": datetime.now(),
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=60),
   
}
#eu.gcr.io/stella-luxury-taxi/transfer-pod1
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

    
    kubernetes_secret_vars_ex = KubernetesPodOperator(
    task_id='ex-kube-secrets',
    name='ex-kube-secrets',
    namespace='default',
    image='alpine',
    cmds=["/bin/bash", "./extract_data.sh", "yellow"],
    # The secrets to pass to Pod, the Pod will fail to create if the
    # secrets you specify in a Secret object do not exist in Kubernetes.
    # env_vars allows you to specify environment variables for your
    # container to use. env_vars is templated.
    env_vars={
        'EXAMPLE_VAR': '/example/value',
        'GOOGLE_APPLICATION_CREDENTIALS': '/var/secrets/google/service-account.json '})
#     from_s3_to_gcs = GKEStartPodOperator(
#         # The ID specified for the task.
#         task_id="data-transfer-task",
#         # Name of task you want to run, used to generate Pod ID.
#         name="data-transfer-task",
#         project_id=PROJECT,
#         location=CLUSTER_REGION,  # type: ignore
#         cluster_name=CLUSTER_NAME,
#         cmds=["/bin/bash/", "echo", "hello"],
# #         cmds=["/bin/bash", "./extract_data.sh", "yellow"],
#         namespace="default",
#         image="alpine",
# #         secrets=[gcp_secret],
#         env_vars={"PROJECT": PROJECT, "STAGING_BUCKET": STAGING_BUCKET, "AWS_CREDS": "/etc/aws/aws-creds.json"
#                  , "GOOGLE_APPLICATION_CREDENTIALS": "/etc/gcp/keyfile.json"},
#     )
    kubernetes_secret_vars_ex
