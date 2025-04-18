# import statements
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash import BashOperator

# Custom Python logic for derriving data value
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# DAG definitions
with DAG(dag_id='Rental_Properties_ETL_DAG',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args
         ) as dag:

    # Dummy strat task   
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Task2
    etl_task= BashOperator(
        task_id='execute_python_ETL_script',
        bash_command='''
            echo "Copying script from GCS to local directory..."
            gsutil cp gs://europe-west8-composer-test-ddf213a4-bucket/dags/scripts/Rental_Properties_ETL.py /home/airflow/gcs/data/
            echo "Script copied successfully."
            echo "Executing Python script..."
            python3 /home/airflow/gcs/data/Rental_Properties_ETL.py
            echo "Python script executed successfully."
        ''',
        dag=dag
    )

    # GCS to BigQuery data load Operator and task
    gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq_load',
        bucket='demo-etl/real-estate-src',
        source_objects=['data.csv'],
        destination_project_dataset_table='PROJECT_ID:DataFlow_DB.rental-etl-demo',
        schema_fields=[
            {      "name": "property_id",      "type": "INTEGER"    },
            {      "name": "city",      "type": "STRING"    },
            {      "name": "postal_code",      "type": "INTEGER"    },
            {      "name": "state_code",      "type": "STRING"    },
            {      "name": "state",      "type": "STRING"    },
            {      "name": "country",      "type": "STRING"    },
            {      "name": "latitude",      "type": "FLOAT"    },
            {      "name": "longitude",      "type": "FLOAT"    },
            {      "name": "beds",      "type": "FLOAT"    },
            {      "name": "baths",      "type": "FLOAT"    },
            {      "name": "sqft",      "type": "FLOAT"    },
            {      "name": "property_type",      "type": "STRING"    },
            {      "name": "list_price",      "type": "FLOAT"    },
            {      "name": "list_date",      "type": "STRING"    },
            {      "name": "last_sold_date",      "type": "STRING"    },
            {      "name": "last_sold_price",      "type": "FLOAT"    },
            {      "name": "create_on",      "type": "STRING"    }
                        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE', 
        dag=dag
    )

    # Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

# Settting up task  dependency
# start >> gcs_to_bq_load >> create_aggr_bq_table >> end
start >> etl_task >> gcs_to_bq_load >> end
