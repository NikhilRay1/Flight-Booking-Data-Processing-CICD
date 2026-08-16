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
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable


# ---------------------------------------------------------
# Default arguments
# ---------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 5, 15),
}


# ---------------------------------------------------------
# DAG
# ---------------------------------------------------------

with DAG(
    dag_id="flight_booking_dataproc_bq_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["flight-booking", "dataproc", "bigquery"],
) as dag:

    # -----------------------------------------------------
    # Airflow Variables
    # -----------------------------------------------------

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

    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]


    # -----------------------------------------------------
    # Unique Dataproc Serverless batch ID
    # -----------------------------------------------------

    batch_id = (
        f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"
    )


    # -----------------------------------------------------
    # Task 1: Wait for input file in GCS
    # -----------------------------------------------------

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


    # -----------------------------------------------------
    # Task 2: Dataproc Serverless PySpark batch
    # -----------------------------------------------------

    batch_details = {

        # -------------------------------------------------
        # PySpark configuration
        # -------------------------------------------------

        "pyspark_batch": {

            "main_python_file_uri": (
                f"gs://{gcs_bucket}/"
                f"flight-booking-analysis/"
                f"spark-job/"
                f"spark_transformation_job.py"
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


        # -------------------------------------------------
        # Dataproc Serverless runtime
        # -------------------------------------------------

        "runtime_config": {

            "version": "2.2",

            "properties": {

                # Minimum supported driver CPU
                "spark.driver.cores": "4",

                # Driver memory
                "spark.driver.memory": "4g",

                # Minimum supported executor CPU
                "spark.executor.cores": "4",

                # Executor memory
                "spark.executor.memory": "4g",

                # Minimum number of executors
                "spark.executor.instances": "2",

                # Prevent autoscaling from requesting
                # additional executors for this small project
                "spark.dynamicAllocation.minExecutors": "2",
                "spark.dynamicAllocation.maxExecutors": "2",
            },
        },


        # -------------------------------------------------
        # Execution environment
        # -------------------------------------------------

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


    # -----------------------------------------------------
    # Submit Dataproc Serverless batch
    # -----------------------------------------------------

    pyspark_task = DataprocCreateBatchOperator(

        task_id="run_spark_job_on_dataproc_serverless",

        batch=batch_details,

        batch_id=batch_id,

        project_id="nikhilgcp-502406",

        region="us-central1",

        gcp_conn_id="google_cloud_default",
    )


    # -----------------------------------------------------
    # Dependency
    # -----------------------------------------------------

    file_sensor >> pyspark_task