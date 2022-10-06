import os
from datetime import datetime

import click

from crypto import utils


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
@click.argument("channel", nargs=1)
def make_reddit(start, end, channel):
    """build text file from reddit"""
    from crypto.TextMaker import RedditMaker

    r = RedditMaker(channel, start, end)

    r.fetch_text_union(channel)


@cli.command()
@click.argument("start", nargs=1)
@click.argument("end", nargs=1)
@click.argument("base", nargs=1)
@click.argument("source", nargs=1)
def make_price(start, end, base="USD", source="CB"):
    """build price file"""

    from crypto.PriceMaker import make_price
    import json
    if source == "CB":
        from crypto.universe import coinbase_universe
        source_universe = coinbase_universe()
        ticker_join = '-'
    elif source == "BN":
        from crypto.universe import binance_universe
        source_universe = binance_universe()
        ticker_join = ''
    else:
        raise Exception(f"source {source} undefined")

    with open(utils.universe_path, 'r') as universe_file:
        universe = json.load(universe_file)

    meta = {}

    for name in universe:
        ticker = ticker_join.join([name, base])
        if ticker not in source_universe:
            print(f"\n{ticker} is not supported in {source}")
            continue
        print(f"\nmaking {ticker}")
        start_, end_, row_count, na_count = make_price(ticker, start, end, source)

        # df = pd.read_hdf(os.path.join(utils.price_path, f"{ticker}.h5"))
        meta[ticker] = {"start": str(start_),
                        "end": str(end_),
                        "count": str(row_count),
                        "na": str(na_count),
                        "update": datetime.strftime(datetime.utcnow(), '%Y-%m-%d %H:%M:%S')}

        with open(os.path.join(utils.price_path, f"{source}/meta.json"), 'r+') as meta_file:
            json.dump(meta, meta_file)


if __name__ == '__main__':
    cli()
