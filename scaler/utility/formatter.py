STORAGE_SIZE_MODULUS = 1024.0
TIME_MODULUS = 1000


def format_bytes(number) -> str:
    for unit in ["b", "k", "m", "g", "t"]:
        if number >= STORAGE_SIZE_MODULUS:
            number /= STORAGE_SIZE_MODULUS
            continue

        if unit in {"b", "k"}:
            return f"{int(number)}{unit}"

        return f"{number:.1f}{unit}"


def format_integer(number):
    return f"{number:,}"


def format_percentage(number: float):
    return f"{number:.1%}"


def format_microseconds(number: int):
    for unit in ["us", "ms", "s"]:
        if number >= TIME_MODULUS:
            number /= TIME_MODULUS
            continue

        if unit == "us":
            return f"{number/TIME_MODULUS:.1f}ms"

        too_big_sign = "+" if unit == "s" and number > TIME_MODULUS else ""
        return f"{int(number)}{too_big_sign}{unit}"


def format_seconds(number: int):
    if number > 60:
        return "60+s"

    return f"{number}s"
