from airflow import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1,
    "retries": 0,
}

dag = DAG(
    "s02-prep_data",
    default_args=default_args,
    schedule_interval=None,
    tags=["e2e example", "ezaf", "spark", "parquet", "mnist"],
    params={
        "export_path": Param(
            "Airflow/parquet-data",
            type="string",
            description="Path to folder on user volume to export processed data for further training",
        ),
        "s3_endpoint": Param(
            "local-s3-service.ezdata-system.svc.cluster.local:30000",
            type="string",
            description="S3 endpoint to pull binary data from",
        ),
        "s3_endpoint_ssl_enabled": Param(
            False, type="boolean", description="Whether to use SSL for S3 endpoint"
        ),
        "s3_bucket": Param(
            "seat02", type="string", description="S3 bucket to pull binary data from"
        ),
        "s3_path": Param(
            "data/mnist",
            type="string",
            description="S3 key to pull binary data from",
        ),
        "airgap_registry_url": Param(
            "",
            type=["null", "string"],
            pattern=r"^$|^\S+/$",
            description="Airgap registry url. Trailing slash in the end is required",
        ),
    },
    render_template_as_native_obj=True, 
    access_control={"Admin": {"can_read"},"Admin.student1032.02-hpelabsonline.com":{"can_read","can_edit","can_delete"}},
)

submit = SparkKubernetesOperator(
    task_id="spark_submit",
    application_file="data_to_parquet.yaml",
#    do_xcom_push=True,
    delete_on_termination=False,
    dag=dag,
    enable_impersonation_from_ldap_user=True,
)

#sensor = SparkKubernetesSensor(
#    task_id="monitor",
#    application_name="{{ task_instance.xcom_pull(task_ids='submit')['metadata']['name'] }}",
#    dag=dag,
#    attach_log=True,
#)
#submit >> sensor