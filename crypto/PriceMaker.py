import os
import time
import warnings
import json
from operator import itemgetter

import cbpro
import numpy as np
import pandas as pd
from tables import NaturalNameWarning
from tqdm import tqdm
import requests
import talib
from talib import abstract

from crypto.utils import price_path, price_config

TECH_GROUPS = ['Cycle Indicators', 'Momentum Indicators', 'Overlap Studies', 'Pattern Recognition', 'Price Transform',
               'Statistic Functions', 'Volatility Indicators', 'Volume Indicators']

warnings.filterwarnings('ignore', category=NaturalNameWarning)

public_client = cbpro.PublicClient()

rate_ctrl_t = []


def get_tech_factor(price):
    tech_factors = itemgetter(*TECH_GROUPS)(talib.get_function_groups())
    tech_factors = [factor for group in tech_factors for factor in group]
    tech_factors.remove('MAVP')

    factor_results = []
    for func in tech_factors:
        factor = eval('abstract.' + func)(price)
        if isinstance(factor, pd.Series):
            factor.name = func
        elif isinstance(factor, pd.DataFrame):
            factor.columns = ['_'.join([func, col]) for col in factor.columns]
        factor_results.append(factor)

    return pd.concat(factor_results, axis=1)


def make_price_tech(ticker, source, freq=None, save=False):
    """
    return clean price & tech indicators
    """
    try:
        price = get_clean_price(ticker, source, freq)
    except Exception as e:
        return f"{ticker} failed", e
    factor = get_tech_factor(price[['open', 'high', 'low', 'close', 'volume']])

    df = pd.concat([price, factor], axis=1).dropna()

    if save:
        if freq is None:
            freq = '1min'
        df.to_hdf(os.path.join(price_path, f"price_clean/{ticker}-{freq}.h5"),
                  key=ticker + '-' + freq,
                  mode='w',
                  format='table',
                  complevel=5)
    else:
        return df


def get_clean_price(ticker, source, freq=None):
    with open(os.path.join(price_path, f"{source}/meta.json"), 'r') as f:
        meta = json.load(f)
    meta = meta.get(ticker)
    start_dt = pd.to_datetime(meta.get('start')).timestamp()

    if not os.path.exists(os.path.join(price_path, f"{source}/{ticker}.h5")):
        raise Exception(f"{ticker} does not exist on disk")
    price_df = pd.read_hdf(os.path.join(price_path, f"{source}/{ticker}.h5"), where=f"epoch>={start_dt}")
    price_df['datetime'] = pd.to_datetime(price_df.epoch, unit='s')
    price_df.set_index('datetime', inplace=True)
    price_df.drop('epoch', axis=1, inplace=True)

    if freq is not None:
        price_df_freq = price_df.resample(freq, label='right', closed='right', origin='start_day').agg(
            {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum', 'amount': 'sum',
             'count': 'sum'}
        )
    else:
        price_df_freq = price_df

    price_df_freq.close.fillna(method='ffill', inplace=True)
    price_df_freq.fillna(
        {'open': price_df_freq.close, 'high': price_df_freq.close, 'low': price_df_freq.close, 'volume': 0, 'amount': 0,
         'count': 0},
        inplace=True
    )
    price_df_freq.dropna(inplace=True)

    return price_df


def make_price(ticker, start, end, source):
    global rate_ctrl_t
    rate_ctrl_t = price_config[source]["rate_limit"] * [time.time()]

    chunk = price_config[source]["chunk_size"]

    prices = pd.DataFrame(columns=price_config[source]["columns"])

    start = pd.to_datetime(start)
    start_ = None
    end = pd.to_datetime(end) + pd.to_timedelta('23H59min')
    bar = tqdm(pd.date_range(start, end, freq=price_config[source]["interval"]))  # x-Hour interval one iteration

    if source == "CB":
        price_func = price_cb
        first_dt = None
    elif source == "BN":
        price_func = price_bn
        first_dt = _price_bn_earliest(ticker, start)
    else:
        raise Exception(f"source {source} undefined")

    chunk_count = 0
    row_count = 0
    na_count = 0

    for s in bar:
        bar.set_description(str(s))
        e = s + pd.to_timedelta(price_config[source]["interval_1"])  # 0H00m -- x-Hour-1min
        if first_dt is not None:
            if e < first_dt:
                continue

        price = price_func(ticker, s, e, bar)
        if type(price) == int:
            if price == -2 and start_ is not None:
                bar.write(f"{ticker} + {s}: no data")
            continue
        row_count += len(price)
        na_count += price.isna().any(axis=1).sum()
        if start_ is None and row_count > 0:
            start_ = pd.to_datetime(price.epoch.min(), unit='s')
        prices = pd.concat([prices, price], axis=0, ignore_index=True)
        # prices = prices.append(price)

        chunk_count += 1
        if chunk_count % chunk == 0:
            save_price(prices, ticker, source)
            del prices
            prices = pd.DataFrame(columns=price_config[source]["columns"])

    if len(prices) > 0:
        save_price(prices, ticker, source)

    return start_, end, row_count, na_count


def __rate_limit():
    global rate_ctrl_t
    t0 = rate_ctrl_t.pop(0)
    t1 = time.time()
    if t1 - t0 < 1:
        time.sleep(1 - (t1 - t0))
    rate_ctrl_t.append(time.time())


def save_price(prices, ticker, source):
    prices = prices.astype({"epoch": int})
    if os.path.exists(os.path.join(price_path, f"{source}/{ticker}.h5")):
        prices.to_hdf(os.path.join(price_path, f"{source}/{ticker}.h5"),
                      key=ticker,
                      mode='a',
                      format='table',
                      index=False,
                      data_columns=["epoch"],
                      append=True)
    else:
        prices.to_hdf(os.path.join(price_path, f"{source}/{ticker}.h5"),
                      key=ticker,
                      mode='w',
                      format='table',
                      index=False,
                      data_columns=["epoch"])


def price_cb(ticker, start, end, bar=None):
    if bar is None:
        bar = tqdm()

    retry_times = 0
    while True:
        try:
            __rate_limit()
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
            continue  # --> retry

        price = np.array(price)
        if price.ndim == 0:  # price matrix not ok: usually hitted the api rate limit
            bar.write(f"{ticker} + {start}")
            bar.write(str(price))
            time.sleep(1)
            continue
        elif price.ndim != 2:
            # bar.write(f"{ticker} + {start}: no data")
            # bar.write(str(price))
            return -2

        price = pd.DataFrame(price[:, 1:].astype(float), index=price[:, 0].astype(int),
                             columns=["high", "low", "open", "close", "volume"])
        price = price.reindex(range(int(start.timestamp()), int(end.timestamp()) + 60, 60))
        price.index.name = "epoch"
        price.reset_index(inplace=True)
        return price


def price_bn(ticker, start, end, bar=None):
    if bar is None:
        bar = tqdm()

    retry_times = 0
    while True:
        try:
            __rate_limit()
            price = requests.get(
                url=f"https://api.binance.com/api/v3/klines?symbol={ticker}&interval=1m"
                    f"&startTime={1000 * int(start.timestamp())}"
                    f"&endTime={1000 * int(end.timestamp())}&limit=1000"
            )
            if price.status_code != 200:
                bar.write(price.status_code)
                bar.write(price.json())
                os.system("pause")
                continue
            price = price.json()
        except Exception as ex:
            retry_times += 1
            if retry_times > 5:
                bar.write(f"{ticker} + {start}: failed")
                bar.write(ex)
                return -1
            time.sleep(0.5)
            continue

        price = np.array(price)
        if price.ndim == 0:  # price matrix not ok: usually hitted the api rate limit
            bar.write(f"{ticker} + {start}")
            bar.write(str(price))
            time.sleep(1)
            continue
        elif price.ndim != 2:
            return -2

        # price[:, 0] = price[:, 0]/1000
        price = np.delete(price, [6, 9, 10, 11], axis=1)

        price = pd.DataFrame(price[:, 1:].astype(float), index=(price[:, 0].astype(np.int64) / 1000).astype(int),
                             columns=["open", "high", "low", "close", "volume", "amount", "count"])
        price = price.reindex(range(int(start.timestamp()), int(end.timestamp()) + 60, 60))
        price.index.name = "epoch"
        price.reset_index(inplace=True)

        return price


def _price_bn_earliest(ticker, start):
    retry_times = 0
    while True:
        try:
            price = requests.get(
                url=f"https://api.binance.com/api/v3/klines?symbol={ticker}&interval=1m"
                    f"&startTime={1000 * int(start.timestamp())}&limit=2"
            )
            if price.status_code != 200:
                print(price.status_code)
                print(price.json())
                os.system("pause")
                continue
            price = price.json()
            break
        except Exception as ex:
            retry_times += 1
            if retry_times > 5:
                return start
            continue

    price = np.array(price)
    if price.ndim != 2:
        return start

    return pd.to_datetime(price[0, 0].astype(np.int64) / 1000, unit='s')


if __name__ == '__main__':
    # make_price("BTCUSDT", '2018-02-01', '2022-09-26', "BN")
    # df = pd.read_hdf('./BTC-USD.h5', key='BTC-USD')
    print(_price_bn_earliest("BTCUSDT", pd.to_datetime("2017-01-01")))
    # print(df, df.dtypes)
    #
    # input()

    # 'BTC-USD', '2021-09-18', '2022-09-18', True
