from pymongo import MongoClient, collection
from data import MongoCredential
from utilities import get_batch_size, load_config
from pipeline_functions import sample
from maps_functions import apply_strategy

from collections.abc import Generator
from typing import Optional
import contextlib
import itertools
import os

@contextlib.contextmanager
def db_get_client(uri: str) -> Generator[MongoClient, None, None]:
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        # Force connection check
        client.admin.command("ping")
        yield client
    finally:
        client.close()
    # When URI is invalid, the client will never be None, it will have hte MongoClient type with "lazy connect"
    # Only trigger error when you try to use it for querying
    
def db_clear(
    db_client: MongoClient,
    db_name: str, 
    collection_name: str
) -> Optional[int]:
    try:
        # Catch invalid type/value
        if not db_name:
            raise TypeError(f"db_name has invalid type, it must be in {type(str)} type")
        if not collection_name:
            raise TypeError(f"collection_name has invalid type, it must be in {type(str)} type")
        if db_name.lower() == "financial_data_prod":
            raise RuntimeError("Refusing to clear production database!")

        # Get DB collection
        db_collection = db_client[db_name][collection_name]

        # Delete DB collection
        delete_result = db_collection.delete_many({})
        return delete_result.deleted_count
    
    except Exception as e:
        raise Exception("The following error occurred: ", e)

def db_copy(
    db_client: MongoClient, 
    source_db_name: str, 
    dest_db_name: str, 
    collection_name: str,
    batch_size: Optional[int] = None
) -> Optional[int]:
    try:
        # Catch invalid type/value
        if not source_db_name or not dest_db_name:
            raise TypeError(f"source_db_name OR dest_db_name has invalid type, it must be in {type(str)} type")
        
        if not collection_name:
            raise TypeError(f"collection_name has invalid type, it must be in {type(str)} type")

        # Get source and dest DBs
        source_db = db_client[source_db_name]
        source_collection = source_db[collection_name]
        dest_db = db_client[dest_db_name]
        dest_collection = dest_db[collection_name]

        # Delete the old data from dest DB just in case
        dest_collection.delete_many({}) 
        # Determine the batch size if not provided
        batch_size = batch_size or get_batch_size(source_collection)
        # Query the data from source DB
        docs_cursor = source_collection.find(filter={}, batch_size=batch_size)

        # Insert multiple batches of docs
        batch = []
        total_inserted = 0
        for doc in docs_cursor:
            batch.append(doc)
            if len(batch) >= batch_size:
                try:
                    dest_collection.insert_many(batch)
                    total_inserted += len(batch)
                except Exception as e:
                    print(f"Batch insert failed: {e}, skipping this batch")
                # Clear the batch, so you won't batch insert the duplicate doc later again
                batch.clear()

        # Insert the rest of batch
        if batch:
            try:
                dest_collection.insert_many(batch)
                total_inserted += len(batch)
            except Exception as e:
                print(f"Batch insert failed: {e}, skipping this batch")

        return total_inserted
                
    except Exception as e:
        raise Exception("The following error occurred: ", e)

def db_sanitize_obfuscate(
    db_client: MongoClient,
    source_db_name: str,
    dest_db_name: str,
    collection_name: str,
    config_rules: dict,
    batch_size: Optional[int] = None
) -> Optional[int]:
    try:
        # Catch invalid type/value
        if not source_db_name or not dest_db_name:
            raise TypeError(f"source_db_name OR dest_db_name has invalid type, it must be in {type(str)} type")  
        if not collection_name:
            raise TypeError(f"collection_name has invalid type, it must be in {type(str)} type")
        if source_db_name.lower() == "financial_data_prod" or dest_db_name.lower() == "financial_data_prod":
            raise RuntimeError("Refusing to sanitize/obfuscate production database!")

        # Get first the source/dest DBs and then source/dest collections
        source_db = db_client[source_db_name]
        source_collection = source_db[collection_name]
        dest_db = db_client[dest_db_name]

        # Create temp collection
        temp_collection_name = f"{collection_name}_temp"
        dest_collection = dest_db[temp_collection_name]
        # Drop previous temp collection if found
        dest_collection.drop()

        # Determine the batch size if not provided
        batch_size = batch_size or get_batch_size(source_collection)
        # Retrieve all documents per collection
        doc_cursor = source_collection.find({}, batch_size=batch_size)
        batch = []

        # Only proceed if there are sanitization/obfuscation rules
        total_inserted = 0
        if config_rules:
            # Iterate through document
            for doc in doc_cursor:
                # Iterate through field and field config
                for field, rules in config_rules.items():
                    # Only proceed if input field exists in a specific doc
                    if field in doc:
                        # All strategy functions standardize to return dict
                        result_dict = apply_strategy(doc[field], rules)
                        # Update or Add field values
                        doc[field] = result_dict["value"]
                        if "salt" in result_dict:
                            doc[f"{field}_salt"] = result_dict["salt"]
                batch.append(doc)

                # Insert multiple batches of docs
                if len(batch) >= batch_size:
                    try:
                        dest_collection.insert_many(batch)
                        total_inserted += len(batch)
                    except Exception as e:
                        print(f"Batch insert failed: {e}, skipping this batch")
                    # Clear the batch, so you won't batch insert the duplicate doc later again
                    batch.clear()

        # Insert the rest of batch
        if batch:
            try:
                dest_collection.insert_many(batch)
                total_inserted += len(batch)
            except Exception as e:
                print(f"Batch insert failed: {e}, skipping this batch")

        # Drop original and rename temp → original
        if source_db_name == dest_db_name:
            # Safe rename --> Same DB: dropTarget ensures atomic replacement
            dest_collection.rename(collection_name, dropTarget=True)
        else:
            # Different DB: drop source manually, then rename
            source_collection.drop()
            dest_collection.rename(collection_name)
        return total_inserted

    except Exception as e:
        raise RuntimeError(f"Error during sanitize/obfuscate: {e}")

def db_minimize(
    db_client: MongoClient, 
    source_db_name: str, 
    dest_db_name: str, 
    collection_name: str,
    sample_percentage: float = 0.1,
    batch_size: Optional[int] = None
) -> Optional[int]:
    
    # --> Use stratified sampling in the future

    try:
        # Catch invalid type/value
        if not source_db_name or not dest_db_name:
            raise TypeError(f"source_db_name OR dest_db_name has invalid type, it must be in {type(str)} type")  
        if not collection_name:
            raise TypeError(f"collection_name has invalid type, it must be in {type(str)} type")
        if source_db_name.lower() == "financial_data_prod" or dest_db_name.lower() == "financial_data_prod":
            raise RuntimeError("Refusing to sanitize/obfuscate production database!")

        # Get first the source/dest DBs and then source/dest collections
        source_db = db_client[source_db_name]
        source_collection = source_db[collection_name]
        dest_db = db_client[dest_db_name]

        # Create temp collection
        temp_collection_name = f"{collection_name}_temp"
        dest_collection = dest_db[temp_collection_name]
        # Drop previous temp collection if found
        dest_collection.drop()

        # Determine the batch size if not provided
        batch_size = batch_size or get_batch_size(source_collection)
        # Perform strategy function --> ONLY Sampling NOW
        sampled_cursor = sample(source_collection, sample_percentage, batch_size)
        
        # Check if the sample size is zero, if it is, clear the DB and stop
        try:
            # Reassign the cursor prevent the iterator to run out of stream data
            sampled_cursor, cursor_copy = itertools.tee(sampled_cursor)
            next(cursor_copy)
        except StopIteration:
            # No docs sampled → clear dest DB collection
            db_clear(db_client, dest_db_name, collection_name)
            return 0

        # Insert multiple batches of docs
        batch = []
        total_inserted = 0
        for doc in sampled_cursor:
            batch.append(doc)
            if len(batch) >= batch_size:
                try:
                    dest_collection.insert_many(batch)
                    total_inserted += len(batch)
                except Exception as e:
                    print(f"Batch insert failed: {e}, skipping this batch")
                # Clear the batch, so you won't batch insert the duplicate doc later again
                batch.clear()
        # Insert the rest of batch
        if batch:
            try:
                dest_collection.insert_many(batch)
                total_inserted += len(batch)
            except Exception as e:
                print(f"Batch insert failed: {e}, skipping this batch")

        # Drop original and rename temp → original
        if source_db_name == dest_db_name:
            # Safe rename --> Same DB: dropTarget ensures atomic replacement
            dest_collection.rename(collection_name, dropTarget=True)
        else:
            # Different DB: drop source manually, then rename
            source_collection.drop()
            dest_collection.rename(collection_name)
        return total_inserted

    except Exception as e:
        raise RuntimeError(f"Error during sampling: {e}")

if __name__ == "__main__":
    # Get DB credentials
    cred = MongoCredential()
    MONGO_URI = f"{cred.protocol}://{cred.host}:{cred.port}{cred.path}"
    # Get DB configurations
    databases_config = load_config()

    # Start the process
    with db_get_client(MONGO_URI) as client:
        # Parse through the DBs
        for index, db_config in enumerate(databases_config):
            # Field Validation 'source_db_name'
            if not db_config.get('source_db_name'):
                raise Exception("Database config " + str(index+1) + " must include a non-empty 'source_db_name' field")
            # Field Validation 'dest_db_name'
            if not db_config.get('dest_db_name'):
                raise Exception(f"Database config " + str(index+1) + " must include a non-empty 'source_db_name' field")

            # Perform data copy
            db_copy(
                client, 
                db_config.get('source_db_name'), 
                db_config.get('dest_db_name'), 
                'accounts'
            ) 
            db_copy(
                client, 
                db_config.get('source_db_name'), 
                db_config.get('dest_db_name'), 
                'customers'
            ) 
            db_copy(
                client, 
                db_config.get('source_db_name'), 
                db_config.get('dest_db_name'), 
                'transactions'
            ) 
        print(db_minimize(
            client, 
            'financial_data_mini', 
            'financial_data_mini', 
            'accounts'
        ))
        print(db_minimize(
            client, 
            'financial_data_mini', 
            'financial_data_mini', 
            'customers'
        ))
        print(db_minimize(
            client, 
            'financial_data_mini', 
            'financial_data_mini', 
            'transactions'
        ))