from pymongo import collection
from psutil import virtual_memory

from typing import Optional
import decimal
import os
import yaml

def get_batch_size(
        collection: collection.Collection, 
        memory_fraction: float=0.3, 
        pystorage_inflation_factor: float=20
) -> int:
    try:
        # Get DB stats
        stats = collection.database.command("collstats", collection.name)
        avg_doc_size = stats.get('avgObjSize')
        docs_count = stats.get('count', 0)

        if avg_doc_size is None or docs_count == 0:
            raise RuntimeError("The DB where you are fetching the batch size from CANNOT BE EMPTY!")

        # Compute virtual memory (in bytes)
        mem = virtual_memory()
        memory_budget = mem.available * memory_fraction

        # Individual doc size in Python storage (in bytes)
        avg_doc_size_python = avg_doc_size * pystorage_inflation_factor

        # Compute batch size
        batch_size = int(min(docs_count, memory_budget / avg_doc_size_python))

        return batch_size

    except Exception as e:
        raise Exception("The following error occurred: ", e)

def get_decimal_places(value: int|float) -> Optional[int]:
    if not isinstance(value, (int, float)):
        raise TypeError("Value must be int or float")

    # Split integer and decimal part from value
    int_deci_split = str(decimal.Decimal(str(value))).split('.')
    # If split array has decimal part, return numbers of decimal, otherwise, integer returns 0
    return len(int_deci_split[1]) if len(int_deci_split) > 1 else 0

def load_config(
    config_path: Optional[str] = None,
    env_var_name: str = "PIPELINE_CONFIG_DIR",
    default_file_name: str = "pipeline_configs.yml"
) -> list[dict]:
    
    # If config_path not specified in the input, use env var to get config dir
    if config_path is None:
        # Fetch config dir
        config_dir = os.getenv(env_var_name)
        # If it works, continues to get the config_path
        if config_dir:
            config_path = os.path.join(config_dir, default_file_name)
        # If config_path not found in env var, try relative path
        else:
            parent_dir = os.path.dirname(os.path.dirname(__file__))
            config_path = os.path.join(parent_dir, "configs", default_file_name)

    # Read YAML
    with open(config_path, 'r') as config_file:
        config_data = yaml.safe_load(config_file)

    # Handle empty or None config
    if not config_data:
        raise ValueError(f"Config file {config_path} is empty or invalid")
    
    # Handle empty or None pipeline field
    pipelines = config_data.get("pipelines")
    if not pipelines:
        raise ValueError(f"Config file {config_path} must include a non-empty 'pipelines' field")
    
    # Check if order field exists in pipeline array
    # If yes, sort it by order
    # if no, put all array element without order at the front in relative order
    return sorted(pipelines, key=lambda x: x.get("order", 0))