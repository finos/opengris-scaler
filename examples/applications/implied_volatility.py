"""
This program computes the implied Black-Scholes volatility given market price and model price.

This program is revised based on
https://stackoverflow.com/questions/61289020/fast-implied-volatility-calculation-in-python

Usage:
    $ git clone https://github.com/finos/opengris-scaler && cd opengris-scaler
    $ pip install -r examples/applications/requirements_applications.txt
    $ python -m examples.applications.implied_volatility
"""

from timeit import default_timer
from typing import List

import numpy as np
import psutil
from scipy.stats import norm

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def bs_call(S, K, T, r, vol):
    d1 = (np.log(S / K) + (r + 0.5 * vol**2) * T) / (vol * np.sqrt(T))
    d2 = d1 - vol * np.sqrt(T)
    return S * norm.cdf(d1) - np.exp(-r * T) * K * norm.cdf(d2)


def bs_vega(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    return S * norm.pdf(d1) * np.sqrt(T)


def find_volatility(target_value, S, K, T, r) -> float:
    MAX_ITERATIONS = 200
    PRECISION = 1.0e-5
    sigma = 0.5
    for _ in range(0, MAX_ITERATIONS):
        price = bs_call(S, K, T, r, sigma)
        vega = bs_vega(S, K, T, r, sigma)
        diff = target_value - price  # our root
        if abs(diff) < PRECISION:
            return sigma
        sigma = sigma + diff / vega  # f(x) / f'(x)
    return sigma  # value wasn't found, return best guess so far


def find_volatilities(dataset: np.ndarray) -> List[float]:
    return [find_volatility(*sample) for sample in dataset]


def generate_synthetic_data(n_samples: int) -> np.ndarray:
    S = np.random.randint(100, 200, n_samples)  # stock prices
    K = S * 1.25  # strike prices
    T = np.ones(n_samples)  # time maturity (year)
    r = np.random.randint(0, 3, n_samples) / 100  # risk-free-rate
    vol = np.random.randint(15, 50, n_samples) / 100  # volatility
    prices = bs_call(S, K, T, r, vol)

    return np.column_stack((prices, S, K, T, r))


def main():
    N_SAMPLES = 20_000
    N_SAMPLES_PER_TASK = 1_000

    n_workers = psutil.cpu_count()
    if n_workers is None:
        n_workers = 4

    cluster = SchedulerClusterCombo(n_workers=n_workers)

    dataset = generate_synthetic_data(N_SAMPLES)

    # Split the dataset in chunks of up to `N_SAMPLES_PER_TASK`.
    per_task_dataset = [
        (dataset[data_begin : data_begin + N_SAMPLES_PER_TASK],)
        for data_begin in range(0, N_SAMPLES, N_SAMPLES_PER_TASK)
    ]

    # Process the dataset in parallel, concatenate the results
    with Client(address=cluster.get_address()) as client:
        start = default_timer()
        results = np.concatenate(client.map(find_volatilities, per_task_dataset))
        duration = default_timer() - start

    cluster.shutdown()

    print(f"Computed {len(results)} stock volatilities in {duration:.2f}s")


if __name__ == "__main__":
    main()
