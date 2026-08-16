# from datetime import datetime, timedelta
# import uuid  # Import UUID for unique batch IDs
# from airflow import DAG
# from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
# from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
# from airflow.models import Variable

# # DAG default arguments
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#     'start_date': datetime(2025, 5, 15),
# }

# # Define the DAG
# with DAG(
#     dag_id="flight_booking_dataproc_bq_dag",
#     default_args=default_args,
#     schedule_interval=None,  # Trigger manually or on-demand
#     catchup=False,
# ) as dag:

#     # Fetch environment variables
#     env = Variable.get("env", default_var="dev")
#     gcs_bucket = Variable.get("gcs_bucket", default_var="airflow-projects-buckett")
#     bq_project = Variable.get("bq_project", default_var="nikhilgcp-502406")
#     bq_dataset = Variable.get("bq_dataset", default_var=f"flight_data_{env}")
#     tables = Variable.get("tables", deserialize_json=True)

#     # Extract table names from the 'tables' variable
#     transformed_table = tables["transformed_table"]
#     route_insights_table = tables["route_insights_table"]
#     origin_insights_table = tables["origin_insights_table"]

#     # Generate a unique batch ID using UUID
#     batch_id = f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"  # Shortened UUID for brevity

#     # # Task 1: File Sensor for GCS
#     file_sensor = GCSObjectExistenceSensor(
#         task_id="check_file_arrival",
#         bucket=gcs_bucket,
#         object=f"flight-booking-analysis/source-{env}/flight_booking.csv",  # Full file path in GCS
#         google_cloud_conn_id="google_cloud_default",  # GCP connection
#         timeout=300,  # Timeout in seconds
#         poke_interval=30,  # Time between checks
#         mode="poke",  # Blocking mode
#     )

#     # Task 2: Submit PySpark job to Dataproc Serverless
#     batch_details = {
#         "pyspark_batch": {
#             "main_python_file_uri": f"gs://{gcs_bucket}/flight-booking-analysis/spark-job/spark_transformation_job.py",  # Main Python file
#             "python_file_uris": [],  # Python WHL files
#             "jar_file_uris": [],  # JAR files
#             "args": [
#                 f"--env={env}",
#                 f"--bq_project={bq_project}",
#                 f"--bq_dataset={bq_dataset}",
#                 f"--transformed_table={transformed_table}",
#                 f"--route_insights_table={route_insights_table}",
#                 f"--origin_insights_table={origin_insights_table}",
#             ]
#         },
#   "runtime_config": {
#     "version": "2.2",
#     "properties": {
#         "spark.driver.cores": "2",
#         "spark.driver.memory": "2g",
#         "spark.executor.cores": "4",
#         "spark.executor.memory": "2g",
#         "spark.executor.instances": "2",
#     },
# },
#         "environment_config": {
#             "execution_config": {
#                 "service_account": "506506058788-compute@developer.gserviceaccount.com",
#                 "network_uri": "projects/nikhilgcp-502406/global/networks/default",
#                 "subnetwork_uri": "projects/nikhilgcp-502406/regions/us-central1/subnetworks/default",
#             }
#         },
#     }

#     pyspark_task = DataprocCreateBatchOperator(
#         task_id="run_spark_job_on_dataproc_serverless",
#         batch=batch_details,
#         batch_id=batch_id,
#         project_id="nikhilgcp-502406",
#         region="us-central1",
#         gcp_conn_id="google_cloud_default",
#     )

#     # Task Dependencies
#     file_sensor >> pyspark_task

# from datetime import datetime, timedelta
# import uuid  # Import UUID for unique batch IDs
# from airflow import DAG
# from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
# from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
# from airflow.models import Variable

# # DAG default arguments
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#     'start_date': datetime(2026, 8, 16),
# }

# # Define the DAG
# with DAG(
#     dag_id="flight_booking_dataproc_bq_dag",
#     default_args=default_args,
#     schedule_interval=None,  # Trigger manually or on-demand
#     catchup=False,
# ) as dag:

#     # Fetch environment variables
#     env = Variable.get("env", default_var="dev")
#     gcs_bucket = Variable.get("gcs_bucket", default_var="airflow-projects-buckett")
#     bq_project = Variable.get("bq_project", default_var="nikhilgcp-502406")
#     bq_dataset = Variable.get("bq_dataset", default_var=f"flight_data_{env}")
#     tables = Variable.get("tables", deserialize_json=True)

#     # Extract table names from the 'tables' variable
#     transformed_table = tables["transformed_table"]
#     route_insights_table = tables["route_insights_table"]
#     origin_insights_table = tables["origin_insights_table"]

#     # Generate a unique batch ID using UUID
#     batch_id = f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"  # Shortened UUID for brevity

#     # # Task 1: File Sensor for GCS
#     file_sensor = GCSObjectExistenceSensor(
#         task_id="check_file_arrival",
#         bucket=gcs_bucket,
#         object=f"flight-booking-analysis/source-{env}/flight_booking.csv",  # Full file path in GCS
#         google_cloud_conn_id="google_cloud_default",  # GCP connection
#         timeout=300,  # Timeout in seconds
#         poke_interval=30,  # Time between checks
#         mode="poke",  # Blocking mode
#     )

#     # Task 2: Submit PySpark job to Dataproc Serverless
#     batch_details = {
#         "pyspark_batch": {
#             "main_python_file_uri": f"gs://{gcs_bucket}/flight-booking-analysis/spark-job/spark_transformation_job.py",  # Main Python file
#             "python_file_uris": [],  # Python WHL files
#             "jar_file_uris": [],  # JAR files
#             "args": [
#                 f"--env={env}",
#                 f"--bq_project={bq_project}",
#                 f"--bq_dataset={bq_dataset}",
#                 f"--transformed_table={transformed_table}",
#                 f"--route_insights_table={route_insights_table}",
#                 f"--origin_insights_table={origin_insights_table}",
#             ]
#         },
#         "runtime_config": {
#     "version": "2.2",
#     "properties": {
#         "spark.driver.cores": "1",
#         "spark.driver.memory": "2g",
#         "spark.executor.cores": "1",
#         "spark.executor.memory": "2g",
#         "spark.executor.instances": "2",
#     },
# },
#         "environment_config": {
#             "execution_config": {
#                 "service_account": "506506058788-compute@developer.gserviceaccount.com",
#                 "network_uri": "projects/nikhilgcp-502406/global/networks/default",
#                 "subnetwork_uri": "projects/nikhilgcp-502406/regions/us-central1/subnetworks/default",
#             }
#         },
#     }

#     pyspark_task = DataprocCreateBatchOperator(
#         task_id="run_spark_job_on_dataproc_serverless",
#         batch=batch_details,
#         batch_id=batch_id,
#         project_id="nikhilgcp-502406",
#         region="us-central1",
#         gcp_conn_id="google_cloud_default",
#     )

#     # Task Dependencies
#     file_sensor >> pyspark_task

from datetime import datetime, timedelta
import uuid

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
)


# ============================================================
# DAG DEFAULT ARGUMENTS
# ============================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 8, 16),
}


# ============================================================
# DAG DEFINITION
# ============================================================

with DAG(
    dag_id="flight_booking_dataproc_bq_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Flight booking PySpark ETL using Dataproc Serverless and BigQuery",
) as dag:

    # ========================================================
    # AIRFLOW VARIABLES
    # ========================================================

    env = Variable.get(
        "env",
        default_var="dev",
    )

    gcs_bucket = Variable.get(
        "gcs_bucket",
        default_var="airflow-projects-buckett",
    )

    bq_project = Variable.get(
        "bq_project",
        default_var="nikhilgcp-502406",
    )

    bq_dataset = Variable.get(
        "bq_dataset",
        default_var=f"flight_data_{env}",
    )

    tables = Variable.get(
        "tables",
        deserialize_json=True,
    )

    # ========================================================
    # BIGQUERY TABLE NAMES
    # ========================================================

    transformed_table = tables["transformed_table"]

    route_insights_table = tables["route_insights_table"]

    origin_insights_table = tables["origin_insights_table"]

    # ========================================================
    # UNIQUE DATAPROC SERVERLESS BATCH ID
    # ========================================================

    batch_id = (
        f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"
    )

    # ========================================================
    # TASK 1: WAIT FOR INPUT FILE IN GCS
    # ========================================================

    file_sensor = GCSObjectExistenceSensor(
        task_id="check_file_arrival",

        bucket=gcs_bucket,

        object=(
            f"flight-booking-analysis/"
            f"source-{env}/"
            f"flight_booking.csv"
        ),

        google_cloud_conn_id="google_cloud_default",

        timeout=300,

        poke_interval=30,

        mode="poke",
    )

    # ========================================================
    # TASK 2: DATAPROC SERVERLESS PYSPARK BATCH
    # ========================================================

    batch_details = {

        # ----------------------------------------------------
        # PYSPARK CONFIGURATION
        # ----------------------------------------------------

        "pyspark_batch": {

            "main_python_file_uri": (
                f"gs://{gcs_bucket}/"
                "flight-booking-analysis/"
                "spark-job/"
                "spark_transformation_job.py"
            ),

            "python_file_uris": [],

            "jar_file_uris": [],

            "args": [
                f"--env={env}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}",
            ],
        },

        # ----------------------------------------------------
        # DATAPROC SERVERLESS RUNTIME CONFIGURATION
        # ----------------------------------------------------

"runtime_config": {
    "version": "2.2",
    "properties": {
        "spark.driver.cores": "4",
        "spark.driver.memory": "4g",
        "spark.executor.cores": "4",
        "spark.executor.memory": "4g",
        "spark.executor.instances": "2",
    },
},

        # ----------------------------------------------------
        # EXECUTION ENVIRONMENT
        # ----------------------------------------------------

        "environment_config": {

            "execution_config": {

                "service_account": (
                    "506506058788-compute@developer.gserviceaccount.com"
                ),

                "network_uri": (
                    "projects/nikhilgcp-502406/"
                    "global/networks/default"
                ),

                "subnetwork_uri": (
                    "projects/nikhilgcp-502406/"
                    "regions/us-central1/"
                    "subnetworks/default"
                ),
            },
        },
    }

    # ========================================================
    # DATAPROC CREATE BATCH OPERATOR
    # ========================================================

    pyspark_task = DataprocCreateBatchOperator(

        task_id="run_spark_job_on_dataproc_serverless",

        batch=batch_details,

        batch_id=batch_id,

        project_id="nikhilgcp-502406",

        region="us-central1",

        gcp_conn_id="google_cloud_default",
    )

    # ========================================================
    # TASK DEPENDENCY
    # ========================================================

    file_sensor >> pyspark_task