import json
import requests
from bs4 import BeautifulSoup
import cbpro

from crypto.utils import universe_path

NAME_CLASS = "d-lg-inline font-normal text-3xs tw-ml-0 md:tw-ml-2 md:tw-self-center tw-text-gray-500 dark:tw-text-white dark:tw-text-opacity-60"
NAME_LONG_CLASS = "lg:tw-flex font-bold tw-items-center tw-justify-between"
NAME_CLASS_STABLE = "d-lg-inline font-normal text-3xs tw-ml-0 md:tw-ml-2 md:tw-self-center tw-text-gray-500"


def fetch_universe(num):
    res = requests.get(url='https://www.coingecko.com/')
    res = BeautifulSoup(res.text, features="lxml")

    res = res.find('div', {'class': 'coingecko-table'}).find('tbody')
    res_short = res.find_all('span', {'class': NAME_CLASS})[:num]
    res_long = res.find_all('span', {'class': NAME_LONG_CLASS})[:num]

    stablecoins = _fetch_stablecoin_universe()

    universe = [[], []]
    for i in range(num):
        if res_short[i].text.strip() in stablecoins:
            continue
        universe[0].append(res_short[i].text.strip())
        universe[1].append(res_long[i].text.strip())

    with open(universe_path, 'w') as universe_file:
        json.dump(universe, universe_file)


def _fetch_stablecoin_universe():
    res = requests.get(url="https://www.coingecko.com/en/categories/stablecoins?")
    res = BeautifulSoup(res.text, features="lxml")

    res = res.find_all('span', {'class': NAME_CLASS_STABLE})

    stablecoin_list = [i.text.strip() for i in res]
    return stablecoin_list


def coinbase_universe():
    public_client = cbpro.PublicClient()
    res = public_client.get_products()
    products = [i['id'] for i in res]
    return products


def binance_universe():
    # NOTE!!! ticker name format from binance is "{base_curr}{quote_curr}". No "-" between names.
    res = requests.get("https://api.binance.com/api/v3/exchangeInfo")
    res = res.json()
    products = [i["symbol"] for i in res["symbols"]]
    return products


if __name__ == '__main__':
    fetch_universe(50)
