"""
This program gets closing price of a given ticker and start dates. This program is revised based on
https://stackoverflow.com/a/77342764

Usage:
    $ git clone https://github.com/finos/opengris-scaler && cd opengris-scaler
    $ pip install -r examples/applications/requirements_applications.txt
    $ python -m examples.applications.yfinance_historical_price
"""

import datetime
import os
from typing import Optional

import pandas as pd
import psutil
import yfinance as yf

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def get_option_data(stock_symbol: str, expiration_date: Optional[str], option_type: str, strike: float):
    stock = yf.Ticker(stock_symbol)
    option_chain = stock.option_chain(expiration_date)
    options = getattr(option_chain, "calls" if option_type.startswith("call") else "puts")
    option_data = options[options["strike"] == strike]
    return option_data


def get_option_history_data(contract_symbol, days_before_expiration: int = 30):
    option = yf.Ticker(contract_symbol)
    option_info = option.info
    option_expiration_date = datetime.datetime.fromtimestamp(option_info["expireDate"])

    start_date = option_expiration_date - datetime.timedelta(days=days_before_expiration)
    option_history = option.history(start=start_date)
    return option_history


def get_option_close_prices_with_strike(strike):
    # from yfinance.base import YFRateLimitError
    # stock_symbol = "AAPL"
    # expiration_date = None  # User may wish to specify expiration_date
    # days_before_expiration = 30
    # option_type = "call"

    # res = []
    # try:
    #     option_data = get_option_data(stock_symbol, expiration_date, option_type, strike)
    #     for _, od in option_data.iterrows():
    #         contract_symbol = od["contractSymbol"]
    #         option_history = get_option_history_data(contract_symbol, days_before_expiration)
    #         first_option_history = option_history.iloc[0]
    #         first_option_history_date = option_history.index[0]
    #         first_option_history_close = first_option_history["Close"]
    #         res.append((contract_symbol, first_option_history_close, first_option_history_date))
    # except YFRateLimitError:
    #     print("Early return due to requesting too many requests.")
    # return res

    # NOTE: Here, we are mocking data that we will be receiving, and pass them back. This is to avoid creating network
    # traffic to a third party. If you wish to get data from Yahoo, comment out below section, and uncomment everything
    # that's above.
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), "downloaded_data.csv"))
    mock_data = [(row["contractSymbol"], float(row["close"]), pd.to_datetime(row["date"])) for _, row in df.iterrows()]

    if strike % 5 != 0:
        return []
    res = [mock_data[(strike - 170) // 5]]
    return res


def main():
    n_workers = psutil.cpu_count()
    if n_workers is None:
        n_workers = 4

    strike_start = 170
    strike_end = 240

    cluster = SchedulerClusterCombo(n_workers=n_workers)

    with Client(address=cluster.get_address()) as client:
        results = client.map(
            get_option_close_prices_with_strike, [(strike,) for strike in range(strike_start, strike_end)]
        )

    cluster.shutdown()

    for lists_of_closing_dates in results:
        for contract_symbol, first_option_history_close, first_option_history_date in lists_of_closing_dates:
            print(
                f"For {contract_symbol}, the closing price was ${first_option_history_close:.2f} on "
                f"{first_option_history_date}."
            )


if __name__ == "__main__":
    main()
