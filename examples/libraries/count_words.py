import argparse
from collections import Counter
from typing import List

import parfun as pf


@pf.parallel(
    split=pf.per_argument(lines=pf.py_list.by_chunk),  # Parallelize by splitting the file content
    combine_with=lambda results: sum(results, start=Counter()),  # Sum the result counters
)
def count_words(lines: List[str]) -> Counter:
    counter: Counter = Counter()
    for line in lines:
        for word in line.split():
            counter[word] += 1
    return counter


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", nargs="?", default=__file__)
    parser.add_argument("--scaler-address", dest="scaler_address", default=None)

    args = parser.parse_args()

    with open(args.filename, "r") as file:
        lines = file.readlines()

    if args.scaler_address:
        # Connects to a remote Scaler Cluster
        with pf.set_parallel_backend_context("scaler_remote", args.scaler_address):
            results = count_words(lines)
    else:
        # Creates a temporary local Scaler cluster
        with pf.set_parallel_backend_context("scaler_local"):
            results = count_words(lines)

    print(results)
