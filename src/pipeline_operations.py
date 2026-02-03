def copy_task(
    source_db_name: str,
    dest_db_name: str,
    collection_name: str
) -> int | None:
    from data import MongoCredential
    from database_operations import db_get_client, db_copy
    cred = MongoCredential()
    MONGO_URI = f"{cred.protocol}://{cred.host}:{cred.port}{cred.path}"
    with db_get_client(MONGO_URI) as client:
        return db_copy(client, source_db_name, dest_db_name, collection_name)

def sanitize_task(
    source_db_name: str, 
    dest_db_name: str, 
    collection_name: str, 
    collection_config: dict
) -> int | None:
    from data import MongoCredential
    from database_operations import db_get_client, db_sanitize_obfuscate
    cred = MongoCredential()
    MONGO_URI = f"{cred.protocol}://{cred.host}:{cred.port}{cred.path}" 
    with db_get_client(MONGO_URI) as client:
        return db_sanitize_obfuscate(client, source_db_name, dest_db_name, collection_name, collection_config)
    
def minimize_task(
    source_db_name: str, 
    dest_db_name: str, 
    collection_name: str, 
    collection_config: dict
) -> int | None:
    from data import MongoCredential
    from database_operations import db_get_client, db_minimize
    cred = MongoCredential()
    MONGO_URI = f"{cred.protocol}://{cred.host}:{cred.port}{cred.path}" 
    with db_get_client(MONGO_URI) as client:
        return db_minimize(client, source_db_name, dest_db_name, collection_name, collection_config["params"]["percentage"])

def dynamic_pipeline_one_shot():
    # -----------------------------
    # Load YAML config
    # -----------------------------
    from utilities import load_config
    step_configs = load_config()

    from data import MongoCredential
    from database_operations import db_get_client, db_copy, db_sanitize_obfuscate, db_minimize

    # -----------------------------
    # Iterate through steps
    # -----------------------------
    from collections import deque
    sequential_pipeline = deque([])

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
        # Dynamic pipeline function calling through configs
        # -----------------------------
        for collection_name, collection_configs in collections.items():
            collection_name_safe = collection_name.replace(" ", "_")

            match step_type:
                case "copy":
                    func = copy_task
                    args = [source_db_name, dest_db_name, collection_name_safe]
                    kwargs = {}
                case "sanitize_obfuscate":
                    func = sanitize_task
                    args = [source_db_name, dest_db_name, collection_name_safe]
                    kwargs = {"collection_config": collection_configs}
                case "minimize":
                    func = minimize_task
                    args = [source_db_name, dest_db_name, collection_name_safe]
                    kwargs = {"collection_config": collection_configs}
                case _:
                    raise ValueError(f"Unsupported step_type: {step_type}")

            sequential_pipeline.append({
                "step_name": step_name,
                "step_type": step_type,
                "collection_name": collection_name_safe,
                "function": func,
                "args": args,
                "kwargs": kwargs,
            })

    # Run sequentially
    while sequential_pipeline:
        task = sequential_pipeline.popleft()
        print(f"Running {task['step_name']} ({task['step_type']}) "
              f"on {task['collection_name']}")
        result = task["function"](*task["args"], **task["kwargs"])
        print(f" -> Result: {result}")

if __name__ == "__main__":
    dynamic_pipeline_one_shot()