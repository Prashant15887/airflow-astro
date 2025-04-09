from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

@dag(start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def fetch_snowflake_data():
    
    @task
    def fetch_data_from_snowflake():
        # Connect to Snowflake
        conn = SnowflakeHook(snowflake_conn_id='snowflake_dev').get_conn() 
        cursor = conn.cursor()
        
        # Fetch data from the "cookies" table in the "public" schema
        cursor.execute('select * from snowflake_sample_data.tpch_sf1.nation')
        data = cursor.fetchall()
        print(data)
        
        # Close the Snowflake connection
        cursor.close()
        conn.close()
        
        return data
    
    @task()
    def transform_data(data):
        return data
    
    @task()
    def store(data):
        print(data)
        
    store(transform_data(fetch_data_from_snowflake()))

dag = fetch_snowflake_data()