from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table='yt_api'

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id='postgres_db_yt_elt', database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()

def create_schema(schema):
    conn, cur = get_conn_cursor()
    
    schema_sql=f"CREATE SCHEMA IF NOT EXISTS {schema};"
    
    cur.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn, cur)
    
def create_table(schema):

    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT   
                );
            """
    else: 
        table_sql = f"""
                  CREATE TABLE IF NOT EXISTS {schema}.{table} (
                      "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                      "Video_Title" TEXT NOT NULL,
                      "Upload_Date" TIMESTAMP NOT NULL,
                      "Duration" TIME NOT NULL,
                      "Video_Type" VARCHAR(10) NOT NULL, 
                      "Video_Views" INT,
                      "Likes_Count" INT,
                      "Comments_Count" INT    
                  ); 
              """

    cur.execute(table_sql)

    conn.commit()

    close_conn_cursor(conn, cur)

def get_video_ids(cur, schema):
    """Return all Video_ID values from the target schema using an open cursor."""
    
    cur.execute(f"SELECT \"Video_ID\" FROM {schema}.{table};")
    ids=cur.fetchall() # Fetches all video IDs from the specified schema and table, returning them as a list of dictionaries.
    video_ids=[row['Video_ID'] for row in ids]# Extracts the video IDs from the list of dictionaries and creates a list of video IDs.
    
    return video_ids