from airflow import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.utils.dates import days_ago

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
    "sxx-spark_read_write_parquet_mnist",
    default_args=default_args,
    schedule_interval=None,
    tags=["e2e example", "ezaf", "spark", "parquet", "mnist"],
    params={
        "export_path": Param(
            "/Airflow/parquet-data",
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
            "seat-xx", type="string", description="S3 bucket to pull binary data from"
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
        "export_path_2": Param(
            "/Airflow/mnist-data",
            type=["null", "string"],
            pattern=r"^$|^\S+/$",
            description="The final directory with the mnist files",
        ),
    },
    render_template_as_native_obj=True, 
    access_control={"Admin": {"can_read","can_edit","can_delete"}},
)

submit = SparkKubernetesOperator(
    task_id="spark submit",
    application_file="example_ezaf_spark_mnist.yaml",
    # do_xcom_push=True,
    delete_on_termination=False,
    dag=dag,
    enable_impersonation_from_ldap_user=True,
)

datamove = KubernetesOperator(
    task_id="data move",
    image="beatbox",
    cmds=["python /mnt/user/Airflow/data-move.py {{dag_run.conf['export_path']}} {{dag_run.conf['export_path_2']}}"],
    volume_mount = k8s.V1VolumeMount(
    name="user-volume", mount_path="/mnt/usr", sub_path=None, read_only=True
    ),
    volume = k8s.V1Volume(
    name="user-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="user-pvc"),
    ),
    delete_on_termination=True,
    dag=dag,
)

# sensor = SparkKubernetesSensor(
#     task_id="monitor",
#     application_name="{{ task_instance.xcom_pull(task_ids='submit')['metadata']['name'] }}",
#     dag=dag,
#     attach_log=True,
# )

submit >> datamove