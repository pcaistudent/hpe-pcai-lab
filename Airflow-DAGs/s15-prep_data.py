from __future__ import annotations
import os
from datetime import datetime
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1,
    "retries": 0,
}

dag = DAG(
    "s15-prep_data",
    default_args=default_args,
    schedule=None,
    tags=["examplee", "aie", "spark", "parquet", "mnist"],
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
            "seat15", type="string", description="S3 bucket to pull binary data from"
        ),
        "s3_path": Param(
            "data/mnist",
            type="string",
            description="S3 key to pull binary data from",
        ),
        "registry_url": Param(
            os.environ.get("AIRGAP_REGISTRY"),
            type=["null", "string"],
            pattern=r"^$|^\S+/$",
            description="Airgap registry url. Trailing slash in the end is required",
        ),
    },
    render_template_as_native_obj=True, 
    access_control={
        "Admin": {"can_read","can_edit","can_delete"}},
        "user-student1032-15": {"can_read","can_edit","can_delete"}
)

submit = SparkKubernetesOperator(
    task_id="spark_submit",
    application_file="data_to_parquet_v1_11.yaml",
#    do_xcom_push=True,
    delete_on_termination=False,
    dag=dag,
    enable_impersonation_from_ldap_user=True,
)
