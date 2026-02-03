from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

import sys, os
src = os.getenv("SOURCE_DIR", "/opt/airflow/src")
if src: 
    if src not in sys.path:
        sys.path.append(src)
else:
    raise ValueError("Environment variable SOURCE_DIR does not have valid value")

@task
def copy_task(
    source_db_name: str,
    dest_db_name: str,
    collection_name: str
) -> int | None:
    # Import modules
    # import os
    # import sys
    # src = os.getenv("SOURCE_DIR")
    # if src:
    #     if src not in sys.path:
    #         sys.path.append(src)
    # else:
    #     raise ImportError("IDK")
    from data import MongoCredential
    from database_operations import db_get_client, db_copy

    # Get credential
    cred = MongoCredential()
    MONGO_URI = f"{cred.protocol}://{cred.host}:{cred.port}{cred.path}"
    with db_get_client(MONGO_URI) as client:
        # Call copy module
        return db_copy(client, source_db_name, dest_db_name, collection_name)

@task
def sanitize_obfuscate_task(
    source_db_name: str, 
    dest_db_name: str, 
    collection_name: str, 
    collection_config: dict
) -> int | None:
    # Import modules
    # import os
    # import sys
    # src = os.getenv("SOURCE_DIR")
    # if src:
    #     if src not in sys.path:
    #         sys.path.append(src)
    # else:
    #     raise ImportError("IDK")
    from data import MongoCredential
    from database_operations import db_get_client, db_sanitize_obfuscate

    # Get credential
    cred = MongoCredential()
    MONGO_URI = f"{cred.protocol}://{cred.host}:{cred.port}{cred.path}" 
    with db_get_client(MONGO_URI) as client:
        # Call sanitize_obfuscate module
        return db_sanitize_obfuscate(client, source_db_name, dest_db_name, collection_name, collection_config)
    
@task
def minimize_task(
    source_db_name: str, 
    dest_db_name: str, 
    collection_name: str, 
    collection_config: dict
) -> int | None:
    # Import modules
    # import os
    # import sys
    # src = os.getenv("SOURCE_DIR")
    # if src:
    #     if src not in sys.path:
    #         sys.path.append(src)
    # else:
    #     raise ImportError("IDK")
    from data import MongoCredential
    from database_operations import db_get_client, db_minimize

    # Get credential
    cred = MongoCredential()
    MONGO_URI = f"{cred.protocol}://{cred.host}:{cred.port}{cred.path}" 
    with db_get_client(MONGO_URI) as client:
        # Call minimize module
        return db_minimize(client, source_db_name, dest_db_name, collection_name, collection_config["params"]["percentage"])

@dag(
    dag_id="build_project_etl_pipeline_finalized",
    description="An ETL Pipeline that flows from prod env DB to dev/stage env DB with pipeline ETL Pipeline: prod -> clean -> mini -> dev",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["MY_DAG"],
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60)
)
def dynamic_pipeline():

    # -----------------------------
    # Load YAML config
    # -----------------------------
    # Import modules
    # import os
    # import sys
    # src = os.getenv("SOURCE_DIR")
    # if src:
    #     if src not in sys.path:
    #         sys.path.append(src)
    # else:
    #     raise ImportError("IDK")    
    from utilities import load_config

    step_configs = load_config()
    previous_taskgroup = None

    # -----------------------------
    # Iterate through steps
    # -----------------------------
    # Extract fields
    for step in step_configs:
        # Handle empty or None fields
        for field in ["type", "step", "source_db", "dest_db"]:
            if field not in step:
                raise ValueError(f"Step config is missing required field: {field}")

        step_name = step["step"].replace(" ", "_")
        step_type = step["type"]
        source_db_name = step["source_db"]
        dest_db_name = step["dest_db"]
        collections = step.get("collections", {})
    
        # -----------------------------
        # TaskGroup for collection-level parallelism
        # -----------------------------
        with TaskGroup(group_id=step_name) as tg:
            for collection_name, collection_configs in collections.items():
                collection_name_safe = collection_name.replace(" ", "_")
                match step_type:
                    case 'copy':
                        copy_task.override(task_id=f"{step_name}_{collection_name_safe}")(
                            source_db_name, dest_db_name, collection_name
                        )
                    case 'sanitize_obfuscate':
                        sanitize_obfuscate_task.override(task_id=f"{step_name}_{collection_name_safe}")(
                            source_db_name, dest_db_name, collection_name, collection_configs
                        )
                    case 'minimize':
                        minimize_task.override(task_id=f"{step_name}_{collection_name_safe}")(
                            source_db_name, dest_db_name, collection_name, collection_configs
                        )
                    case _:
                        raise ValueError(f"Unsupported step_type: {step_type}")

        # Chain sequentially by order
        if previous_taskgroup:
            previous_taskgroup >> tg
        previous_taskgroup = tg

# Instantiate DAG
dag = dynamic_pipeline()