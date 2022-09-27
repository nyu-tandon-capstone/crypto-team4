import click
from crypto import utils
import os
from datetime import datetime
import time


@click.group()
def cli():
    pass


@cli.command()
def update_universe():
    """update universe"""
    from crypto.universe import fetch_universe

    fetch_universe(utils.universe_size)


@cli.command()
@click.argument("start", nargs=1)
@click.argument("end", nargs=1)
def make_price(start, end, base="USD"):
    """build price file"""

    from crypto.price_maker import make_price
    from crypto.universe import coinbase_universe
    import json
    import pandas as pd

    with open(utils.universe_path, 'r') as universe_file:
        universe = json.load(universe_file)[base]
    cb_universe = coinbase_universe()

    meta = {}

    for ticker in universe:
        if ticker not in cb_universe:
            print(f"{ticker} is not supported in coinbase")
            continue
        print(f"\nmaking {ticker}")
        start_, end_, row_count, na_count = make_price(ticker, start, end)

        # df = pd.read_hdf(os.path.join(utils.price_path, f"{ticker}.h5"))
        meta[ticker] = {"start": str(start_),
                        "end": str(end_),
                        "count": str(row_count),
                        "na": str(na_count),
                        "update": datetime.strftime(datetime.utcnow(), '%Y-%m-%d %H:%M:%S')}

        with open(os.path.join(utils.price_path, "meta.json"), 'r+') as meta_file:
            json.dump(meta, meta_file)


if __name__ == '__main__':
    cli()
