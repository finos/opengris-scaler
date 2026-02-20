from typing import Any

STORAGE_SIZE_MODULUS = 1024.0
TIME_MODULUS = 1000


def format_bytes(number) -> str:
    for unit in ["B", "K", "M", "G", "T"]:
        if number >= STORAGE_SIZE_MODULUS:
            number /= STORAGE_SIZE_MODULUS
            continue

        if unit in {"B", "K"}:
            return f"{int(number)}{unit}"

        return f"{number:.1f}{unit}"

    raise ValueError("This should not happen")


def format_integer(number):
    return f"{number:,}"


def format_percentage(number: int):
    return f"{(number/1000):.1%}"


def format_microseconds(number: int):
    for unit in ["us", "ms", "s"]:
        if number >= TIME_MODULUS:
            number = int(number / TIME_MODULUS)
            continue

        if unit == "us":
            return f"{number/TIME_MODULUS:.1f}ms"

        too_big_sign = "+" if unit == "s" and number > TIME_MODULUS else ""
        return f"{int(number)}{too_big_sign}{unit}"


def format_seconds(number: int):
    if number > 60:
        return "60+s"

    return f"{number}s"


def to_camel_case(snake_str: str) -> str:
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def to_snake_case(camel_str: str) -> str:
    import re

    # Handle the case where the string is already snake_case or mostly lower
    # But standard CamelCase to snake_case regex
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
