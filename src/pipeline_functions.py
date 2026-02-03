from pymongo import collection
from maps_fields import SyntheticSubMap, RegexPatternMap
from utilities import get_decimal_places

from typing import Iterator, Optional, Any
import os
import hashlib
import re
import random
from datetime import date, datetime

# ** Data Obsfuscating Functions **
def substitute(
    value: Any, 
    field_type: str
) -> Optional[dict[str, Any]]:
    try:
        # Generate faker value
        val = SyntheticSubMap[field_type]()

        # Convert date -> datetime for MongoDB
        if isinstance(val, date) and not isinstance(val, datetime):
            val = datetime(val.year, val.month, val.day)

        return {"value": val}

    except KeyError:
        raise ValueError(f"Unsupported field type for synthetic data: {field_type}")
    
    except Exception as e:
        raise RuntimeError(f"Unexpected error while generating synthetic data: {e}")
     
def perturb(
    value: int|float, 
    noise_percent: float=0.05
) -> dict[str, int|float]:
    
    try:
        if not isinstance(value, (int, float)):
            raise TypeError("Value must be int or float")

        # Compute perturbed value
        noise = value * noise_percent
        perturb_value = value + random.uniform(-noise, noise)

        # Round up value and determine to return int/float
        decimal_places = get_decimal_places(value)
        rounded_value = round(perturb_value, decimal_places)

        # Convert back to int if no decimals
        final_value = int(rounded_value) if decimal_places == 0 else rounded_value
        return {"value": final_value}

    except Exception as e:
        raise RuntimeError(f"Error in perturb: {e}")

def hash_salt(
    value: str,
    salt: Optional[str]=None,
    algorithm: str="sha256"
) -> dict[str, str]:

    # If salt is NoneGenerate a random 16-byte salt
    if salt is None:
        salt = os.urandom(16).hex()

    # Combine value + salt
    value_to_hash = f"{value}{salt}".encode("utf-8")

    # Use hashing algorithm
    hashed_value = hashlib.new(algorithm, value_to_hash).hexdigest()

    return {"value": hashed_value, "salt": salt}

# ** Data Sanitization Functions **
def redact(
    value: str, 
    **params
) -> Optional[dict[str, str]]:
    try:
        # Case 1: Use preset
        if "preset" in params:
            if params["preset"] not in RegexPatternMap:
                raise ValueError(f"Unknown preset: {params['preset']}")
            preset = RegexPatternMap[params["preset"]]
            regex, replacement = preset.get("pattern"), preset.get("replacement")

        # Case 2: Inline config
        else:
            regex = params.get("regex")
            replacement = params.get("replacement")

        # Validate presence
        if not regex or not replacement:
            raise ValueError(f"Missing 'regex' or 'replacement' in params: {params}")

        # Apply redaction
        redacted = re.sub(regex, replacement, str(value))
        return {"value": redacted}

    except re.error as e:
        raise ValueError(f"Invalid regex pattern '{regex}': {e}") # type: ignore
    except Exception as e:
        raise RuntimeError(f"Error in redact: {e}")

def field_removal(value: str):
    return {"value": value}

def doc_removal(value: str):
    return {"value": value}

# ** Data Minimization Functions **
def sample(
    collection: collection.Collection,
    sample_percentage: float,
    batch_size: int
) -> Iterator[dict]:

    # --> Knowledge-driven Array/Object sampling In the Future
    
    # Count documents to compute sample size
    count = collection.estimated_document_count()
    sample_size = int(count * sample_percentage)

    print(f"Now sample size is {sample_size}")

    # If sample size is zero, use empty iterator to signal data minimization to clear dest DB
    if sample_size <= 0:
        return iter([])

    # Use MongoDB $sample aggregation
    sampled_cursor = collection.aggregate(
        [{"$sample": {"size": sample_size}}],
        batchSize=batch_size
    )
    return sampled_cursor
