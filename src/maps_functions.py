from pipeline_functions import substitute, redact, perturb, hash_salt, field_removal, doc_removal, sample

from typing import Any

SanitizationObfuscationFuncMap = {
    "substitute": substitute,
    "redact": redact,
    "perturb": perturb,
    "hash_salt": hash_salt,
    "field_removal": field_removal,
    "doc_removal": doc_removal
}

MinimizationFuncMap = {
    "sample": sample
}

def apply_strategy(
    value: Any, 
    rule: dict
) -> Any:
    # Get the strategy type
    strategy = rule["strategy"]
    if strategy not in SanitizationObfuscationFuncMap:
        raise ValueError(f"Unknown strategy: {strategy}")
    
    # Get the mapped function for strategy, store the function in a variable
    func = SanitizationObfuscationFuncMap[strategy]

    # Return function return values
    return func(value, **rule.get("params", {}))