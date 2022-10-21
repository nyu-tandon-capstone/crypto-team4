price_path = "./data/price/"
text_path = "./data/text/"
universe_path = "./data/universe.json"
universe_size = 50

stablecoin_list = ['UST', 'DAI', 'GUSD', 'USDT', 'USDC', 'MUSD', 'WBTC', 'GYEN', 'CBETH', 'BUSD', 'PAX']

price_config = {
    "BN": {
        "columns": ["epoch", "open", "high", "low", "close", "volume", "amount", "count"],
        "interval": "8H",
        "interval_1": "7H59min",
        "rate_limit": 10,
        "chunk_size": 4
    },
    "CB": {
        "columns": ["epoch", "high", "low", "open", "close", "volume"],
        "interval": "4H",
        "interval_1": "3H59min",
        "rate_limit": 10,
        "chunk_size": 6
    }}

reddit_auth_path = "./redd_auth.json"
