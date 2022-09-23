import os
import time

import cbpro
import click
import numpy as np
import pandas as pd
from tqdm import tqdm

public_client = cbpro.PublicClient()
path = "../data/price/"


@click.command()
@click.option('-t', '--ticker')
@click.option('-s', '--start')
@click.option('-e', '--end')
@click.option('--save_dt', is_flag=True)
def make(ticker, start, end, save_dt=False):
    # init prices & dates array
    prices = np.empty([0, 5])
    dts = np.empty(0, dtype=int)

    bar = tqdm(pd.date_range(start, end, freq='4H'))  # 4H interval one iteration
    count = 0
    t0 = time.time()
    for s in bar:
        bar.set_description(str(s))
        e = s + pd.to_timedelta('3H59min')  # 0H00m -- 3H59m

        t = 0  # failed time
        while True:
            try:
                price = public_client.get_product_historic_rates(
                    ticker,
                    start=str(s),
                    end=str(e),
                    granularity=60
                )  # price from cbpro api
            except Exception as ex:  # get failed
                # failed -- retry
                t += 1
                if t > 5:  # timeout
                    print(s)
                    raise ex
                # reset timer
                t0 = time.time()
                count = 0
                # cool down
                time.sleep(0.5)
                continue  # --> retry

            price = np.array(price)
            if price.ndim == 0:  # price matrix not ok: usually hitted the api rate limit
                print(s)
                print(price)
                # reset timer
                t0 = time.time()
                count = 0
                time.sleep(1)  # cool down
                continue
            break

        price = pd.DataFrame(price[:, 1:], index=price[:, 0].astype(int))
        price = price.reindex(range(int(s.timestamp()), int(e.timestamp()), 60))

        if save_dt:
            dts = np.append(dts, price.index.to_numpy(dtype=int))
        prices = np.append(prices, price.values.astype(float), axis=0)

        # check if close to rate limit
        count += 1
        if count / (time.time() - t0) >= 10:
            time.sleep(0.1)

    # save to .npy file
    if save_dt:
        np.save(os.path.join(path, 'date.npy'), dts)
    np.save(os.path.join(path, f'{ticker}.npy'), prices)


if __name__ == '__main__':
    make()

    # 'BTC-USD', '2021-09-18', '2022-09-18', True

