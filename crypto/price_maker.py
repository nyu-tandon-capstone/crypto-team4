import os
import time
import warnings

import cbpro
import numpy as np
import pandas as pd
from tables import NaturalNameWarning
from tqdm import tqdm

from crypto.utils import price_path

CHUNK = 6

warnings.filterwarnings('ignore', category=NaturalNameWarning)

public_client = cbpro.PublicClient()

t0 = time.time()
count = 0


def make_price(ticker, start, end):
    global t0
    global count

    t0 = time.time()

    prices = pd.DataFrame(columns=["epoch", "high", "low", "open", "close", "volume"])

    start = pd.to_datetime(start)
    start_ = None
    end = pd.to_datetime(end) + pd.to_timedelta('23H59min')
    bar = tqdm(pd.date_range(start, end, freq='4H'))  # 4H interval one iteration
    chunk_count = 0
    row_count = 0
    na_count = 0
    for s in bar:
        bar.set_description(str(s))
        e = s + pd.to_timedelta('3H59min')  # 0H00m -- 3H59m

        price = price_cb(ticker, s, e, bar)
        if type(price) == int:
            # bar.write(f"{ticker} + {s} failed")
            continue
        row_count += len(price)
        na_count += price.isna().any(axis=1).sum()
        if start_ is None and row_count > 0:
            start_ = pd.to_datetime(price.epoch.min(), unit='s')
        prices = pd.concat([prices, price], axis=0, ignore_index=True)
        # prices = prices.append(price)

        chunk_count += 1
        if chunk_count % CHUNK == 0:
            save_price(prices, ticker)
            prices = pd.DataFrame(columns=["epoch", "high", "low", "open", "close", "volume"])

        count += 1
        if count / (time.time() - t0) >= 10:
            time.sleep(0.1)

    if len(prices) > 0:
        save_price(prices, ticker)

    return start_, end, row_count, na_count


def save_price(prices, ticker):
    prices = prices.astype({"epoch": int})
    if os.path.exists(os.path.join(price_path, f"{ticker}.h5")):
        prices.to_hdf(os.path.join(price_path, f"{ticker}.h5"),
                      key=ticker,
                      mode='a',
                      format='table',
                      index=False,
                      data_columns=["epoch"],
                      append=True)
    else:
        prices.to_hdf(os.path.join(price_path, f"{ticker}.h5"),
                      key=ticker,
                      mode='w',
                      format='table',
                      index=False,
                      data_columns=["epoch"])


def price_cb(ticker, start, end, bar=None):
    global t0
    global count

    if bar is None:
        bar = tqdm()

    retry_times = 0
    while True:
        try:
            price = public_client.get_product_historic_rates(
                ticker,
                start=str(start),
                end=str(end),
                granularity=60
            )  # price from cbpro api
        except Exception as ex:  # failed
            # failed -- retry
            retry_times += 1
            if retry_times > 5:  # timeout
                # return f"{ticker} + {start}: failed\n" + str(ex)
                bar.write(f"{ticker} + {start}: failed")
                bar.write(ex)
                return -1
            # cool down
            time.sleep(0.5)
            # reset timer
            t0 = time.time()
            count = 0
            continue  # --> retry

        price = np.array(price)
        if price.ndim == 0:  # price matrix not ok: usually hitted the api rate limit
            bar.write(f"{ticker} + {start}")
            bar.write(str(price))
            time.sleep(1)  # cool down
            # reset timer
            t0 = time.time()
            count = 0
            continue
        elif price.ndim != 2:
            bar.write(f"{ticker} + {start}: no data")
            # bar.write(str(price))
            return -1

        price = pd.DataFrame(price[:, 1:].astype(float), index=price[:, 0].astype(int),
                             columns=["high", "low", "open", "close", "volume"])
        price = price.reindex(range(int(start.timestamp()), int(end.timestamp())+60, 60))
        price.index.name = "epoch"
        price.reset_index(inplace=True)
        return price


if __name__ == '__main__':
    make_price("BTC-USD", '2022-09-24', '2022-09-26')
    # df = pd.read_hdf('./BTC-USD.h5', key='BTC-USD')
    # print(df, df.dtypes)
    #
    # input()

    # 'BTC-USD', '2021-09-18', '2022-09-18', True

