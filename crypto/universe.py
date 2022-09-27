import json
import requests
from bs4 import BeautifulSoup
import cbpro

from crypto.utils import universe_path

NAME_NAME = "d-lg-inline font-normal text-3xs tw-ml-0 md:tw-ml-2 md:tw-self-center tw-text-gray-500 dark:tw-text-white dark:tw-text-opacity-60"


def fetch_universe(num):
    res = requests.get(url='https://www.coingecko.com/')
    res = BeautifulSoup(res.text, features="lxml")

    res = res.find('div', {'class': 'coingecko-table'}).find('tbody')
    res = res.find_all('span', {'class': NAME_NAME})[:num]

    universe = {"USD": [], "BTC": []}
    for name in res:
        universe["USD"].append(name.text.strip() + "-USD")
        universe["BTC"].append(name.text.strip() + "-BTC")

    with open(universe_path, 'w') as universe_file:
        json.dump(universe, universe_file)


def coinbase_universe():
    public_client = cbpro.PublicClient()
    res = public_client.get_products()
    products = [i['id'] for i in res]
    return products


if __name__ == '__main__':
    fetch_universe(50)
