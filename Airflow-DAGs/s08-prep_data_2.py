from airflow import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os

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

def data_move(**kwargs):
    source=kwargs["source"]
    dest=kwargs["dest"]
    print("Initial training data folder is: "+source)
    if not os.path.exists(dest):
        os.makedirs(dest)
        print("created "+dest)
    with open(dest + "/train-images-idx3-ubyte.gz", 'wb') as f1, \
        open(dest + "/t10k-images-idx3-ubyte.gz", 'wb') as f2, \
        open(dest + "/train-labels-idx1-ubyte.gz", 'wb') as f3, \
        open(dest + "/t10k-labels-idx1-ubyte.gz", 'wb') as f4:
            mnist_parquet = pd.read_parquet(source)
            x_train, x_test, y_train, y_test = mnist_parquet["content"]
            f1.write(x_train)
            f2.write(x_test)
            f3.write(y_train)
            f4.write(y_test)
    print("Wrote files to "+dest)
    
dag = DAG(
    "s08-prep_data_2",
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
            "seat08", type="string", description="S3 bucket to pull binary data from"
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
            "Airflow/mnist-data",
            type=["null", "string"],
            description="The final directory with the mnist files",
        ),
    },
    render_template_as_native_obj=True, 
    access_control={"Admin": {"can_read","can_edit","can_delete"}},
)

submit = SparkKubernetesOperator(
    task_id="spark_submit",
    application_file="data_to_parquet.yaml",
    # do_xcom_push=True,
    delete_on_termination=False,
    dag=dag,
    enable_impersonation_from_ldap_user=True,
)

datamove = PythonOperator(
    task_id="data_move",
    python_callable=data_move,
    op_kwargs={"source":"{{dag_run.conf['export_path']}}", "dest":"{{dag_run.conf['export_path_2']}}"},
    dag=dag,
)

# sensor = SparkKubernetesSensor(
#     task_id="monitor",
#     application_name="{{ task_instance.xcom_pull(task_ids='submit')['metadata']['name'] }}",
#     dag=dag,
#     attach_log=True,
# )

submit >> datamove
