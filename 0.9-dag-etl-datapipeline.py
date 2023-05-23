# import the libraries
from datetime import datetime , timedelta
import pytz
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
#trigger class from airflow
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd 
import numpy as np


def data_cleaning_csv():
    vehicle_data = pd.read_csv("/opt/airflow/dags/data/raw/staging/vehicle-data.csv", 
                   names=["Rowid", "Timestamp", "Vehicle_number", "Vehicle_type","doors","tag"])
    vehicle_data.head()
    vehicle_data.info()
    vehicle_data.describe()
    
    #checking null values in columns
    vehicle_data.isnull().sum() 
    
    #replace index values with rowid
    cleaned_data = vehicle_data[["Rowid", "Timestamp", "Vehicle_number","Vehicle_type"]]
    cleaned_data.set_index("Rowid" , inplace=True)
    cleaned_data.head()
    
    cleaned_data.to_csv("/opt/airflow/dags/data/clean/csv_data.csv", index=True)

def data_cleaning_tsv():
    tsv_data = pd.read_table("/opt/airflow/dags/data/raw/staging/tollplaza-data.tsv",sep='\t',
                         names=["Rowid", "Timestamp", "Vehicle_number", "Vehicle_type","Number_axles","Tollplaza_code","Tollplaza_id"])
    tsv_data.head()
    tsv_data.info()
    tsv_data.describe()
    
    #checking null values in columns
    tsv_data.isnull().sum() 
    
    #replace index values with rowid
    cleaned_data = tsv_data[["Number_axles","Tollplaza_code","Tollplaza_id"]]
    cleaned_data.set_index("Number_axles" , inplace=True)
    cleaned_data.head()
    
    cleaned_data.to_csv("/opt/airflow/dags/data/clean/tsv_data.csv", index=True)
    
def data_cleaning_fwf():
    txt_data = pd.read_fwf("/opt/airflow/dags/data/raw/staging/payment-data.txt",
                        names=["day","month","day_number", "Timestamp","Year" ,"Vehicle_number", "code","Vehicle_code","Tollplaza_id","Type_Payment_code"])
    txt_data.head()
    txt_data.info()
    txt_data.describe()
    
    #checking null values in columns
    txt_data.isnull().sum() 
    
    #replace index values with rowid
    cleaned_data = txt_data[["Type_Payment_code","Vehicle_code" ]]
    cleaned_data.set_index("Type_Payment_code" , inplace=True)
    cleaned_data.head()
    
    cleaned_data.to_csv("/opt/airflow/dags/data/clean/fixed_width_data.csv", index=True)
    
def data_transform():
    extracted_data = pd.read_csv("/opt/airflow/dags/data/clean/extracted_data.csv"),
    extracted_data["Vehicle_type"] = extracted_data["Vehicle_type"].apply(str.upper)
    extracted_data.head() 
    
    extracted_data.to_csv("/opt/airflow/dags/data/clean/transformed_data_.csv", index=False)
    
    

def cleaned_data_message():
    print("Data succesfully cleaned")


#defining DAG arguments
default_args = {
    'owner': 'Asael Solorio',
    'start_date':datetime(2023,5,19).astimezone(pytz.UTC),
    'email': ['solorioasael@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
# Context Manager
with DAG(dag_id="DAG-etl_toll_data",
        description="Apache Airflow Final Assignment",
        schedule_interval="@once",
        default_args=default_args,
        start_date=datetime(2023,5,19).astimezone(pytz.UTC),
        end_date=datetime(2023,5,21).astimezone(pytz.UTC),
        catchup=False,
        max_active_runs=1) as dag:

# define the tasks

    unzip_data = BashOperator(task_id="tarea1",
                bash_command="tar -xvzf /opt/airflow/dags/data/raw/staging/tolldata.tgz",
                trigger_rule=TriggerRule.ALL_SUCCESS,
                retries=2,
                retry_delay=5,
                depends_on_past=False,
                cwd="/opt/airflow/dags/data/raw/staging/")
     
    extract_data_from_csv = PythonOperator(task_id="data_cleaning_csv",
                python_callable=data_cleaning_csv,
                retries=2,
                retry_delay=5,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                depends_on_past=True)
    
    extract_data_from_tsv = PythonOperator(task_id="data_cleaning_tsv",
                python_callable=data_cleaning_tsv,
                retries=2,
                retry_delay=5,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                depends_on_past=True)
    
    extract_data_from_fwf = PythonOperator(task_id="data_cleaning_fwf",
                python_callable=data_cleaning_fwf,
                retries=2,
                retry_delay=5,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                depends_on_past=True)
    
    consolidate_data = BashOperator(task_id="consolidando_data",
                bash_command="paste -d ',' /opt/airflow/dags/data/clean/csv_data.csv /opt/airflow/dags/data/clean/tsv_data.csv /opt/airflow/dags/data/clean/fixed_width_data.csv > /opt/airflow/dags/data/clean/extracted_data.csv",
                trigger_rule=TriggerRule.ALL_SUCCESS,
                retries=2,
                retry_delay=5,
                depends_on_past=True)
    
    #transform_data = PythonOperator(task_id="data_transform_csv",
                #python_callable=data_transform,
                #retries=2,
                #retry_delay=5,
                #trigger_rule=TriggerRule.ALL_SUCCESS,
                #depends_on_past=True)
    

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fwf >> consolidate_data 

