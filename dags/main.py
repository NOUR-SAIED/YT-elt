import pendulum
from airflow import DAG
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data , save_data_to_json
from datetime import datetime, timedelta
from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Define the local timezone
local_tz = pendulum.timezone("Africa/Tunis")

#Default arguments for the DAG
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

staging_schema = "staging"
core_schema = "core"

# Define the DAG
with DAG (
    dag_id="produce_json",
    default_args=default_args,
    description="A DAG to produce json",
    schedule="0 14 * * *",
    catchup=False #not to catch up on past runs when the DAG is first created or when it is turned on after being off for a while.
) as dag_produce:
    #define tasks 
    playlist_id=get_playlist_id()
    video_ids=get_video_ids(playlist_id)
    extract_data=extract_video_data(video_ids)
    save_to_json=save_data_to_json(extract_data)
    
    trigger_update_db=TriggerDagRunOperator( # TriggerDagRunOperator is an Airflow operator that allows you to trigger another DAG from within a DAG. In this case, we are using it to trigger the "update_db" DAG after the "produce_json" DAG has completed its tasks.
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
        )
    
    #dependency between tasks
    playlist_id >> video_ids >> extract_data >> save_to_json >> trigger_update_db
    
    
with DAG (
    dag_id="update_db",
    default_args=default_args,
    description="Dag to process JSON file and insert data into both staging an dcore table",
    schedule=None, # we set the schedule to None because we want this DAG to be triggered by the "produce_json" DAG after it has completed its tasks, rather than running on a regular schedule.
    catchup=False #not to catch up on past runs when the DAG is first created or when it is turned on after being off for a while.
) as dag_update:
    
    #define tasks 
    update_staging=staging_table()
    update_core=core_table()
    
    trigger_data_quality_checks=TriggerDagRunOperator( # TriggerDagRunOperator is an Airflow operator that allows you to trigger another DAG from within a DAG. In this case, we are using it to trigger the "data_quality_checks" DAG after the "update_db" DAG has completed its tasks.
        task_id="trigger_data_quality_checks",
        trigger_dag_id="data_quality_checks",
        )
    
    #dependency between tasks
    update_staging >> update_core >> trigger_data_quality_checks
    
with DAG (
    dag_id="data_quality_checks",
    default_args=default_args,
    description="Dag to perform data quality checks using SODA",
    schedule=None, # we set the schedule to None because we want this DAG to be triggered by the "update_db" DAG after it has completed its tasks, rather than running on a regular schedule.
    catchup=False #not to catch up on past runs when the DAG is first created or when it is turned on after being off for a while.
) as dag_quality:
        
    #define tasks 
    soda_validate_staging=yt_elt_data_quality(staging_schema)
    soda_validate_core=yt_elt_data_quality(core_schema)
    
    #dependency between tasks
    soda_validate_staging >> soda_validate_core