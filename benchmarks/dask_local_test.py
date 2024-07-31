import random

from dask.distributed import Client, LocalCluster

from scaler.utility.logging.scoped_logger import ScopedLogger
from scaler.utility.logging.utility import setup_logger


def sleep_print(sec: int):
    return sec * 1


def main():
    setup_logger()
    tasks = [random.randint(0, 100) for _ in range(10000)]

    cluster = LocalCluster(n_workers=10, threads_per_worker=2, memory_limit="100GB")
    client = Client(address=cluster.scheduler_address)

    with ScopedLogger(f"submit {len(tasks)} tasks"):
        futures = [client.submit(sleep_print, i) for i in tasks]

    with ScopedLogger(f"gather {len(futures)} results"):
        results = [future.result() for future in futures]

    assert results == tasks


if __name__ == "__main__":
    main()
