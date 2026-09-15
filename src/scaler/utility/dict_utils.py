import re
from typing import Any, Dict


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Return a new dict that is *base* deep-merged with *override*.

    Nested dicts merge recursively; lists replace entirely; scalars use override.
    """
    result: Dict[str, Any] = dict(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = deep_merge(result[key], val)
        else:
            result[key] = val
    return result


def to_camel_case(snake_str: str) -> str:
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def to_snake_case(camel_str: str) -> str:
    pattern = re.compile(r"(?<!^)(?=[A-Z])")
    return pattern.sub("_", camel_str).lower()


def camelcase_dict(d: Any) -> Any:
    if isinstance(d, dict):
        new_d = {}
        for k, v in d.items():
            new_key = to_camel_case(k) if isinstance(k, str) else k
            new_d[new_key] = camelcase_dict(v)
        return new_d
    elif isinstance(d, list):
        return [camelcase_dict(i) for i in d]
    else:
        return d


def snakecase_dict(d: Any) -> Any:
    if isinstance(d, dict):
        new_d = {}
        for k, v in d.items():
            new_key = to_snake_case(k) if isinstance(k, str) else k
            new_d[new_key] = snakecase_dict(v)
        return new_d
    elif isinstance(d, list):
        return [snakecase_dict(i) for i in d]
    else:
        return d
