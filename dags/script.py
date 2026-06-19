###This script is being used to learn and practice airflow and its functionalities. The script is designed to fetch data from an API and store it in a PostgreSQL database using Airflow DAGs.it will have lots of comments as i am using it to learn and practice airflow and its functionalities.###
import json #used to parse JSON data
import requests 
from datetime import datetime, timedelta #core for working with dates and times
from airflow import DAG #core for creating and managing DAGs 
from airflow.operators.python import PythonOperator #core for creating and managing Python operators
from airflow.providers.postgres.hooks.postgres import PostgresHook #used to connect to PostgreSQL database and execute SQL queries
from airflow.providers.postgres.operators.postgres import PostgresOperator #used to build a sql DB 

#essentially default arguments handles the error handling and retrying of tasks in the DAG. It is a dictionary that contains default parameters for the tasks in the DAG.every part will review and inherit this 
default_args = { 
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
#list of the API endpoints to fetch data from and store in the PostgreSQL database. Each endpoint is represented as a dictionary with the following keys:
endpoints=[
    {
        "id": "Sessions",
        "url": "https://api.openf1.org/v1/sessions",
        "table_name": "f1_sessions"
    },
    {
        "id": "Drivers",
        "url": "https://api.openf1.org/v1/drivers",
        "table_name": "f1_drivers"
    },
    {
        "id": "Laps",
        "url": "https://api.openf1.org/v1/laps",
        "table_name": "f1_laps"
    },
    {
        "id": "Circuits",
        "url": "https://api.openf1.org/v1/circuits",
        "table_name": "f1_circuits" 
    },
    {  
     "id":"location",
     "url":"https://api.openf1.org/v1/location",
     "table_name":"f1_location"
     },
    {
    "id":"session_result",
    "url":"https://api.openf1.org/v1/session_result",
     "table_name":"f1_session_result"   
    },
    {"id":"race_control",
     "url":"https://api.openf1.org/v1/race_control",
     "table_name":"f1_race_control"
     }
]
    
def fetch_data_from_api(url, table_name, **kwargss): #function to fetch data from the API and store it in the PostgreSQL database
    execution_date = kwargss['data_interval_end']#get the execution date of the DAG run from the context and store it in a variable
    
    start_window = execution_date - timedelta(weeks=3) #calculate the start of the time window for the data to be fetched from the API and store it in a variable
    end_window = execution_date.strftime('%Y-%m-%d') #calculate the end of the time window for the data to be fetched from the API and store it in a variable
    
    filtered_url = f"{url}?date_start={start_window.strftime('%Y-%m-%d')}&date_end={end_window}" #create a filtered URL to fetch data from the API for the specified time window and store it in a variable
    print(f"Fetching data from {filtered_url} and storing it in {table_name}")
    
    response = requests.get(filtered_url) #send a GET request to the API endpoint and store the response in a variable   
    data = response.json() #parse the response as JSON and store it in a variable   
    
    if not data:
        print(f"No records found within window for {table_name}. exiting.")
        return
     
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    for r in data: 
        json_string = json.dumps(r) #convert the dictionary to a JSON string
        check_query = f"SELECT 1 FROM {table_name} WHERE data = %s LIMIT 1;"
        cursor.execute(check_query, (json_string,))
        already_exists = cursor.fetchone()
        
        if not already_exists:
            insert_query = f"INSERT INTO {table_name} (data) VALUES (%s)"
            cursor.execute(insert_query, (json_string,))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Successfully sync'd raw API records for {table_name}.")
        
with DAG(
    'f1_data_pipeline', #name of the DAG
    default_args=default_args,
    description='A simple DAG to fetch data from an API and store it in a PostgreSQL database', 
    schedule_interval='@weekly', #schedule interval for the DAG
    catchup=False, #whether to catch up on missed runs
    max_active_runs=2,#maximum number of active runs for the DAG
) as dag:
    for endpoint in endpoints: 
        create_table_task = PostgresOperator(
            task_id=f"create_table_{endpoint['id']}", # Create a unique task id for this loop turn
            postgres_conn_id="postgres_default",
            sql=f"""
            CREATE TABLE IF NOT EXISTS {endpoint['table_name']} (
                id SERIAL PRIMARY KEY,
                data JSONB NOT NULL
            );
            """ # SQL query to create a table dynamically if it does not exist
        ) 
        fetch_data_task = PythonOperator(
                task_id=f"fetch_{endpoint['id']}_data", #task id for the task
                python_callable=fetch_data_from_api, #function to call when the task is executed
                op_kwargs={
                'url': endpoint['url'], 
                'table_name': endpoint['table_name'] #arguments to pass to the function
            },
        )
        create_table_task >> fetch_data_task #set the task dependencies so that the create_table_task runs before the fetch_data_task


