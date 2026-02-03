# [START import_module]
import os
import sys
import subprocess
import logging
import requests
import re
import textwrap
import json
from datetime import datetime, timedelta

from airflow.decorators import dag, task
    # from airflow.sdk import dag, task
    # from airflow.providers.standard.operators.bash import BashOperator
    # from airflow.providers.postgres.operators.postgres import PostgresOperator
    # from airflow.providers.postgres.hooks.postgres import PostgresHook
    # from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    # from airflow.utils.dates import days_ago

    # from pymongo import MongoClient
    # import pendulum
# [END import_module]

# [START instantiate_dag]
@dag(
    dag_id = "build_project_etl_pipeline_draft",
    description="An ETL Pipeline that flows from prod env DB to dev/stage env DB with pipeline ETL Pipeline: prod -> clean -> mini -> dev",
    dagrun_timeout=timedelta(minutes=60),
    schedule = None,
    catchup = False,
    tags=["MY_DAG"],
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    }
)
def data_pipeline_workflow():

# [END instantiate_dag]

    # [START task_definition]
    @task()
    def extract_from_prod_env():
        print("Extract data from MongoDB Production Env")
        return "Data Extraction"

    @task()
    def copy_module(input):
        print(f"Pass down from [{input}]")
        print("Here is Data Copy Module")
        return "Data Copying"

    @task()
    def select_module(input):
        print(f"Pass down from [{input}]")
        print("Here is Data Selection Module")
        return "Data Selection"

    @task()
    def sanitize_module(input):
        print(f"Pass down from [{input}]")
        print("Here is Data Sanitization Module")
        return "Data Sanitization"

    @task()
    def obfuscate_module(input):
        print(f"Pass down from [{input}]")
        print("Here is Data Copy Module")
        return "Data Obfuscation"

    @task()
    def minimize_module(input):
        print(f"Pass down from [{input}]")
        print("Here is Data Minimization Module")
        return "Data Minimization"

    @task()
    def refresh_module(input):        
        print(f"Pass down from [{input}]")
        print("Here is Data Refresh Module")
        return "Data Refreshing"

    @task()
    def restore_module(input):
        print(f"Pass down from [{input}]")
        print("Here is Data Refresh Module")
        return "Data Restoration"

    @task()
    def load_to_other_env(input):
        print(f"Pass down from [{input}]")
        print("Load data to MongoDB Dev/Stage/Sandbox Envs")
        return "Data Loading to MongoDB"
    # [END task_definition]

    # [START creating_dag]
    output_extract =  extract_from_prod_env()
    output_copy = copy_module(output_extract)
    output_select = select_module(output_copy)
    output_sanitize = sanitize_module(output_select)
    output_obfuscate = obfuscate_module(output_sanitize)
    output_minimize = minimize_module(output_obfuscate)
    output_refresh = refresh_module(output_minimize)
    output_restore = restore_module(output_minimize)
    output_load1 = load_to_other_env(output_refresh)
    output_load2 = load_to_other_env(output_restore)
    # [END creating_dag]

dag = data_pipeline_workflow()