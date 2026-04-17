import pendulum
from airflow import DAG
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data , save_data_to_json
from datetime import datetime, timedelta
from datawarehouse.dwh import staging_table, core_table


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

# Define the DAG
with DAG (
    dag_id="produce_json",
    default_args=default_args,
    description="A DAG to produce json",
    schedule="0 14 * * *",
    catchup=False #not to catch up on past runs when the DAG is first created or when it is turned on after being off for a while.
) as dag:
    #define tasks 
    playlist_id=get_playlist_id()
    video_ids=get_video_ids(playlist_id)
    extract_data=extract_video_data(video_ids)
    save_to_json=save_data_to_json(extract_data)
    
    #dependency between tasks
    playlist_id >> video_ids >> extract_data >> save_to_json
    
    
with DAG (
    dag_id="update_db",
    default_args=default_args,
    description="Dag to process JSON file and insert data into both staging an dcore table",
    schedule="0 15 * * *",
    catchup=False #not to catch up on past runs when the DAG is first created or when it is turned on after being off for a while.
) as dag:
    #define tasks 
    update_staging=staging_table()
    update_core=core_table()
    
    #dependency between tasks
    update_staging >> update_core