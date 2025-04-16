"""
This example uses the Prophet library with Scaler to perform parallelized cross-validation.
This reveals a ~4x speedup over the non-parallelized version.
"""

import pandas as pd
from prophet import Prophet
import prophet.diagnostics
from timeit import default_timer as timer
from multiprocessing import cpu_count
from scaler import SchedulerClusterCombo, Client


def main():
    cluster = SchedulerClusterCombo(n_workers=cpu_count())
    client = Client(address=cluster.get_address())

    # Ensure that the client is connected before proceeding
    client.submit(lambda _: ..., None).result()

    # Load the data
    df = pd.read_csv(
        "https://raw.githubusercontent.com/facebook/prophet/master/examples/example_wp_log_peyton_manning.csv",
        parse_dates=["ds"]
    )

    model = Prophet(daily_seasonality=False)

    # Scaler is not involved in the fitting process
    model.fit(df)

    # This adapts the Scaler client to the Prophet diagnostics API
    class Adapter:
        def __init__(self, client: Client):
            self.client = client

        def map(self, func, *iterables):
            return self.client.map(func, [args for args in zip(*iterables)])

    # non-parallelized cross-validation
    start = timer()
    prophet.diagnostics.cross_validation(
        model,
        initial="730 days",
        period="180 days",
        horizon="365 days",
        parallel=None
    )
    non_parallel_time = timer() - start

    # Parallelized cross-validation via Scaler
    start = timer()
    prophet.diagnostics.cross_validation(
        model,
        initial="730 days",
        period="180 days",
        horizon="365 days",
        parallel=Adapter(client)
    )
    parallel_time = timer() - start

    cluster.shutdown()

    print("-" * 30)
    print(f"Non-parallel time: {non_parallel_time:.2f}s")
    print(f"Parallel time: {parallel_time:.2f}s")
    print(f"Speedup: {non_parallel_time / parallel_time:.1f}x")


if __name__ == "__main__":
    main()
