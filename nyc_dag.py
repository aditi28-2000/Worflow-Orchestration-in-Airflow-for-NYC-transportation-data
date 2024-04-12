from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
import numpy as np
import psycopg2

def combine_datasets(**kwargs):
    data_2018 = pd.read_json("/mnt/home/axm1849/airflow/dags/data/2018_data.json")
    data_2019 = pd.read_json("/mnt/home/axm1849/airflow/dags/data/2019_data.json")
    data_2020 = pd.read_json("/mnt/home/axm1849/airflow/dags/data/2020_data.json")
    combined_data = pd.concat([data_2018, data_2019, data_2020], ignore_index=True)
    return combined_data

def clean_dataset(**kwargs):
    # Retrieve the combined dataset from XCom
    ti = kwargs['ti']
    combined_data = ti.xcom_pull(task_ids='combine_datasets')

    # Convert combined dataset to DataFrame
    df_combined = pd.DataFrame(combined_data)

    # Convert data types
    data_types = {
        'vendorid': 'category',
        'tpep_pickup_datetime': 'datetime64[ns]',
        'tpep_dropoff_datetime': 'datetime64[ns]',
        'passenger_count': 'int',
        'trip_distance': 'float',
        'ratecodeid': 'category',
        'store_and_fwd_flag': 'category',
        'pulocationid': 'int',
        'dolocationid': 'int',
        'payment_type': 'category',
        'fare_amount': 'float',
        'extra': 'float',
        'mta_tax': 'float',
        'tip_amount': 'float',
        'tolls_amount': 'float',
        'improvement_surcharge': 'float',
        'total_amount': 'float',
        'congestion_surcharge': 'float'}

    df_combined = df_combined.astype(data_types)
    
    # Define a function to replace years
    def replace_year(dt):
        if dt.year not in [2018, 2019, 2020]:
            return dt.replace(year=2018)
        return dt

    # Apply the function to replace years in 'tpep_pickup_datetime' and 'tpep_dropoff_datetime' columns
    df_combined['tpep_pickup_datetime'] = df_combined['tpep_pickup_datetime'].apply(replace_year)
    df_combined['tpep_dropoff_datetime'] = df_combined['tpep_dropoff_datetime'].apply(replace_year)

    # Fill missing values in 'congestion_surcharge' column with 0
    df_combined['congestion_surcharge'].fillna(0, inplace=True)

    # Save DataFrame to CSV file
    # df_combined.to_csv('cleaned_dataset.csv', index=False)

    return df_combined

def load_database(**kwargs):
    # Retrieve the cleaned dataset from XCom
    ti = kwargs['ti']
    cleaned_data = ti.xcom_pull(task_ids='clean_dataset')

    # Replace 'nan' values with None (NULL)
    #cleaned_data.replace('nan', np.nan, inplace=True)

    # Connect to your MySQL database using the provided connection string
    connection_string = "mysql+pymysql://root:90FFupOIIDg2Sw@129.22.23.234:3312/airflow_db"
    engine = create_engine(connection_string)

    # Manually create the table in MySQL if not exists
    table_name = 'nyc_transportation'

    # Drop the table if it exists
    drop_table_sql = "DROP TABLE IF EXISTS {}".format(table_name)
    with engine.connect() as connection:
        connection.execute(drop_table_sql)

    # Construct the SQL CREATE TABLE statement
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS {} (
            vendorid INT,
            tpep_pickup_datetime DATETIME,
            tpep_dropoff_datetime DATETIME,
            passenger_count INT,
            trip_distance FLOAT,
            ratecodeid INT,
            store_and_fwd_flag VARCHAR(255),
            pulocationid INT,
            dolocationid INT,
            payment_type INT,
            fare_amount FLOAT,
            extra FLOAT,
            mta_tax FLOAT,
            tip_amount FLOAT,
            tolls_amount FLOAT,
            improvement_surcharge FLOAT,
            total_amount FLOAT,
            congestion_surcharge FLOAT
        )
    """.format(table_name)

    # Execute the SQL statement to create the table
    with engine.connect() as connection:
        connection.execute(create_table_sql)

    # Iterate through DataFrame rows and insert into MySQL table
    with engine.connect() as connection:
        for index, row in cleaned_data.iterrows():
            # Construct SQL INSERT statement
            insert_sql = f"""
                INSERT INTO {table_name} 
                (vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, 
                trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, 
                payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, 
                improvement_surcharge, total_amount, congestion_surcharge) 
                VALUES 
                ('{row['vendorid']}', '{row['tpep_pickup_datetime']}', '{row['tpep_dropoff_datetime']}', 
                {row['passenger_count']}, {row['trip_distance']}, {row['ratecodeid']}, 
                '{row['store_and_fwd_flag']}', {row['pulocationid']}, {row['dolocationid']}, 
                {row['payment_type']}, {row['fare_amount']}, {row['extra']}, {row['mta_tax']}, 
                {row['tip_amount']}, {row['tolls_amount']}, {row['improvement_surcharge']}, 
                {row['total_amount']}, {row['congestion_surcharge']})
            """

            # Execute SQL statement
            connection.execute(insert_sql)

def generate_report_1(**kwargs):

    # SQL query to calculate the average number of trips recorded each day per month
    sql_query = """
        SELECT year, month, AVG(num_trips) AS avg_trips_per_day
        FROM (
           SELECT YEAR(tpep_pickup_datetime) AS year,
               MONTH(tpep_pickup_datetime) AS month,
               DAY(tpep_pickup_datetime) AS day,
               COUNT(*) AS num_trips
           FROM nyc_transportation
           GROUP BY year, month, day
        ) AS daily_trips
        GROUP BY year, month ORDER BY year, month;
    """
    # Connect to the MySQL database
    engine = create_engine("mysql+pymysql://root:90FFupOIIDg2Sw@129.22.23.234:3312/airflow_db")
    
    # Execute the SQL query
    with engine.connect() as conn:
        result = conn.execute(sql_query)
        rows = result.fetchall()
        
    # Process the retrieved data
    report_data = pd.DataFrame(rows, columns=['Year', 'Month', 'AverageTripsPerDay'])
    
    # Output the report
    report_data.to_csv('report_1.csv', index=False)

def generate_report_2(**kwargs):

    # SQL query to calculate the average number of trips recorded each day per month
    sql_query = """
        SELECT YEAR(tpep_pickup_datetime) AS year,MONTH(tpep_pickup_datetime) AS month, DAY(tpep_pickup_datetime) AS day,
        ROUND(SUM(fare_amount+extra+mta_tax+tolls_amount+improvement_surcharge+congestion_surcharge),2) AS farebox_amount FROM nyc_transportation
        GROUP BY year, month, day ORDER BY year, month, day;
    """
    # Connect to the MySQL database
    engine = create_engine("mysql+pymysql://root:90FFupOIIDg2Sw@129.22.23.234:3312/airflow_db")

    # Execute the SQL query
    with engine.connect() as conn:
        result = conn.execute(sql_query)
        rows = result.fetchall()

    # Process the retrieved data
    report_data = pd.DataFrame(rows, columns=['Year', 'Month', 'Day', 'FareboxPerDay'])

    # Output the report
    report_data.to_csv('report_2.csv', index=False)

def generate_report_3(**kwargs):

    # SQL query to calculate the average number of trips recorded each day per month
    sql_query = """
        SELECT YEAR(tpep_pickup_datetime) AS year,
        MONTH(tpep_pickup_datetime) AS month,
        pulocationid, dolocationid, COUNT(pulocationid)AS trips
        FROM nyc_transportation
        GROUP BY year, month, pulocationid, dolocationid
        ORDER BY year, month
    """
    # Connect to the MySQL database
    engine = create_engine("mysql+pymysql://root:90FFupOIIDg2Sw@129.22.23.234:3312/airflow_db")

    # Execute the SQL query
    with engine.connect() as conn:
        result = conn.execute(sql_query)
        rows = result.fetchall()

    # Process the retrieved data
    report_data = pd.DataFrame(rows, columns=['Year', 'Month','PickupLocationID', 'DropoffLocationID', 'Trips'])

    # Output the report
    report_data.to_csv('report_3.csv', index=False)


def generate_report_4(**kwargs):

    # SQL query to calculate the average number of trips recorded each day per month
    sql_query = """
        SELECT YEAR(tpep_pickup_datetime) AS year,
        MONTH(tpep_pickup_datetime) AS month,
        ROUND(SUM(trip_distance),2) AS total_trip_miles
        FROM nyc_transportation
        GROUP BY year, month
        ORDER BY year, month;
    """
    # Connect to the MySQL database
    engine = create_engine("mysql+pymysql://root:90FFupOIIDg2Sw@129.22.23.234:3312/airflow_db")

    # Execute the SQL query
    with engine.connect() as conn:
        result = conn.execute(sql_query)
        rows = result.fetchall()

    # Process the retrieved data
    report_data = pd.DataFrame(rows, columns=['Year', 'Month','TotalTripMiles'])

    # Output the report
    report_data.to_csv('report_4.csv', index=False)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'nyc_dag',
    default_args=default_args,
    description='Ingest data from NYC transportation JSON APIs and combine into one dataset',
    schedule_interval=None,
)

ingest_2018_data = BashOperator(
    task_id='ingest_2018_data',
    bash_command="wget -O /mnt/home/axm1849/airflow/dags/data/2018_data.json https://data.cityofnewyork.us/resource/t29m-gskq.json",
    dag=dag,
)

ingest_2019_data = BashOperator(
    task_id='ingest_2019_data',
    bash_command="wget -O /mnt/home/axm1849/airflow/dags/data/2019_data.json https://data.cityofnewyork.us/resource/2upf-qytp.json",
    dag=dag,
)

ingest_2020_data = BashOperator(
    task_id='ingest_2020_data',
    bash_command="wget -O /mnt/home/axm1849/airflow/dags/data/2020_data.json https://data.cityofnewyork.us/resource/kxp8-n2sj.json",
    dag=dag,
)

combine_datasets = PythonOperator(
    task_id='combine_datasets',
    python_callable=combine_datasets,
    provide_context=True,
    dag=dag,
)

clean_dataset = PythonOperator(
    task_id='clean_dataset',
    python_callable=clean_dataset,
    provide_context=True,
    dag=dag,
)

load_database = PythonOperator(
    task_id='load_database',
    python_callable=load_database,
    provide_context=True,
    dag=dag,
)

generate_report_1 = PythonOperator(
    task_id='generate_report_1',
    python_callable=generate_report_1,
    provide_context=True,
    dag=dag
)

generate_report_2 = PythonOperator(
    task_id='generate_report_2',
    python_callable=generate_report_2,
    provide_context=True,
    dag=dag
)

generate_report_3 = PythonOperator(
    task_id='generate_report_3',
    python_callable=generate_report_3,
    provide_context=True,
    dag=dag
)

generate_report_4 = PythonOperator(
    task_id='generate_report_4',
    python_callable=generate_report_4,
    provide_context=True,
    dag=dag
)

[ingest_2018_data, ingest_2019_data, ingest_2020_data] >> combine_datasets >> clean_dataset >> load_database >> [generate_report_1, generate_report_2, generate_report_3, generate_report_4]

